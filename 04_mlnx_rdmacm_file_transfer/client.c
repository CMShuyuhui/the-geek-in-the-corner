#include <fcntl.h>
#include <sys/stat.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <rdma/rdma_cma.h>


const char *DEFAULT_PORT = "12345";
const size_t BUFFER_SIZE = 10 * 1024 * 1024;
const int TIMEOUT_IN_MS = 500;

enum message_id
{
    MSG_INVALID = 0,
    MSG_MR,// 同步mr数据
    MSG_READY,//同步告知client,server已可接收数据
    MSG_DONE//同步告知client,server 准备断开
};

struct message
{
    int id;

    // 交流rkey之用
    union
    {
        struct
        {
            uint64_t addr;
            uint32_t rkey;
        } mr;
    } data;
};



struct client_context
{
    char *buffer;
    struct ibv_mr *buffer_mr;

    struct message *msg;
    struct ibv_mr *msg_mr;

    uint64_t peer_addr;
    uint32_t peer_rkey;

    int fd;
    const char *file_name;
};

struct context 
{
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};
static struct context *s_ctx = NULL;

static void on_completion(struct ibv_wc *wc);
static void on_pre_conn(struct rdma_cm_id *id);


void rc_disconnect(struct rdma_cm_id *id)
{
    rdma_disconnect(id);
}

void rc_die(const char *reason)
{
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

struct ibv_pd* rc_get_pd()
{
    return s_ctx->pd;
}


void * poll_cq(void *ctx)
{
    struct ibv_cq *cq;
    struct ibv_wc wc;
    int ret = 0 ;
    while (1) 
    {

        ret = ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx);
        if (0 != ret )
        {
            fprintf(stderr,"poll_cq ibv_get_cq_event error: %s\n",strerror(errno));
            exit(EXIT_FAILURE);
        }

        ibv_ack_cq_events(cq, 1);
        ret = ibv_req_notify_cq(cq, 0);
        if (0 != ret)
        {
            fprintf(stderr,"poll_cq ibv_req_notify_cq error: %s\n",strerror(errno));
            exit(EXIT_FAILURE);
        }

        while (ibv_poll_cq(cq, 1, &wc)) 
        {
            if (wc.status == IBV_WC_SUCCESS)
            {
                on_completion(&wc);
            }
            else
            {
                rc_die("poll_cq: status is not IBV_WC_SUCCESS");
            }
        }
    }

    return NULL;
}


void build_context(struct ibv_context *verbs)
{
    if (s_ctx) 
    {   
        if (s_ctx->ctx != verbs)
        {
            rc_die("cannot handle events in more than one context.");
        }
        return;
    }

    s_ctx = (struct context *)malloc(sizeof(struct context));

    s_ctx->ctx = verbs;

    s_ctx->pd = ibv_alloc_pd(s_ctx->ctx);
    if (NULL == s_ctx->pd)
    {
        rc_die("ibv_alloc_pd error\n");
    }
    
    s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx);
    if ( NULL == s_ctx->comp_channel )
    {
        rc_die("ibv_create_comp_channel error");
    }
    
    s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0); /* cqe=10 is arbitrary */
    if ( NULL == s_ctx->cq )
    {
        rc_die("ibv_create_cq error");
    }

    int ret = 0;
    ret = ibv_req_notify_cq(s_ctx->cq, 0);
    if (0 != ret)
    {
        fprintf(stderr,"ibv_req_notify_cq error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    ret = pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL);
    if (0 != ret )
    {
        rc_die("can not pthread_create to poll_cq");
    }

}

void build_params(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = 1;
    params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void build_connection(struct rdma_cm_id *id)
{
    struct ibv_qp_init_attr qp_attr;

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    int ret = 0;
    ret = rdma_create_qp(id, s_ctx->pd, &qp_attr);
    if (0 != ret )
    {
        fprintf(stderr,"rdma_create_qp error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
}


void event_loop(struct rdma_event_channel *ec)
{
    struct rdma_cm_event *event = NULL;
    struct rdma_conn_param cm_params;
    int ret = 0;

    build_params(&cm_params);

    while (rdma_get_cm_event(ec, &event) == 0) 
    {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED) // client
        {
            build_connection(event_copy.id);// deal with cc,cq,qp,pd

            on_pre_conn(event_copy.id);// deal with mr

            ret = rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS);
            if (0 != ret )
            {
                fprintf(stderr,"rdma_resolve_route error: %s\n",strerror(errno));
                exit(EXIT_FAILURE);
            }
        } 
        else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED) // client
        {

            ret = rdma_connect(event_copy.id, &cm_params);
            if (0 != ret )
            {
                fprintf(stderr,"rdma_connect error: %s\n",strerror(errno));
                exit(EXIT_FAILURE);
            }
        } 
        else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) 
        {
            printf("rdma_get_cm_event() == RDMA_CM_EVENT_ESTABLISHED, just do nothing\n");
        } 
        else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) 
        {
            rdma_destroy_qp(event_copy.id);

            // rc_disconnect(event_copy.id);

            rdma_destroy_id(event_copy.id);
            break;
        } 
        else 
        {
            rc_die("rdma_get_cm_event() got a unknown event\n");
        }
    }
}


void rc_client_loop(const char *host, const char *port, void *context)
{
    struct addrinfo *addr;
    struct rdma_cm_id *conn = NULL;
    struct rdma_event_channel *ec = NULL;
    struct rdma_conn_param cm_params;

    int ret = 0;

    ret = getaddrinfo(host, port, NULL, &addr);
    if (0 != ret )
    {
        fprintf(stderr,"getaddrinfo error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    ec = rdma_create_event_channel();
    if (NULL == ec)
    {
        fprintf(stderr,"rdma_create_event_channel error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    ret = rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP);
    if (0 != ret)
    {
        fprintf(stderr,"rdma_create_id error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    ret = rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS);
    if (0 != ret )
    {
        fprintf(stderr,"rdma_resolve_addr error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    freeaddrinfo(addr);

    conn->context = context;

    build_params(&cm_params);

    event_loop(ec); // exit on disconnect

    rdma_destroy_event_channel(ec);
}



static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
    struct client_context *ctx = (struct client_context *)id->context;

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)id;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(len);
    wr.wr.rdma.remote_addr = ctx->peer_addr;
    wr.wr.rdma.rkey = ctx->peer_rkey;

    if (len) 
    {
        wr.sg_list = &sge;
        wr.num_sge = 1;

        sge.addr = (uintptr_t)ctx->buffer;
        sge.length = len;
        sge.lkey = ctx->buffer_mr->lkey;
    }

    int ret = ibv_post_send(id->qp, &wr, &bad_wr);
    if (0 != ret )
    {
        fprintf(stderr,"ibv_post_send error : %s\n", strerror(errno));
        exit(EXIT_FAILURE);
    }

}

static void post_receive(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *)id->context;

    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)id;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t)ctx->msg;
    sge.length = sizeof(*ctx->msg);
    sge.lkey = ctx->msg_mr->lkey;

     int ret = ibv_post_recv(id->qp, &wr, &bad_wr);
     if ( 0 != ret )
     {
         fprintf(stderr,"ibv_post_recv error : %s\n",strerror(errno));
         exit(EXIT_FAILURE);
     }

}

static void send_next_chunk(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *)id->context;

    ssize_t size = 0;

    size = read(ctx->fd, ctx->buffer, BUFFER_SIZE);

    if (size == -1)
    {
        rc_die("read() file failed\n");
    }

    write_remote(id, size);
}

static void send_file_name(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *)id->context;

    strcpy(ctx->buffer, ctx->file_name);

    write_remote(id, strlen(ctx->file_name) + 1);
}

static void on_pre_conn(struct rdma_cm_id *id)
{
    struct client_context *ctx = (struct client_context *)id->context;

    posix_memalign((void **)&ctx->buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
    ctx->buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if (NULL == ctx->buffer_mr)
    {
        rc_die("ctx->buffer_mr ibv_reg_mr() error\n");
    }


    posix_memalign((void **)&ctx->msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->msg));
    ctx->msg_mr = ibv_reg_mr(rc_get_pd(), ctx->msg, sizeof(*ctx->msg), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (NULL == ctx->msg_mr)
    {
        rc_die("ctx->msg_mr ibv_reg_mr() error\n");
    }
    post_receive(id);
}

static void on_completion(struct ibv_wc *wc)
{
    struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)(wc->wr_id);
    struct client_context *ctx = (struct client_context *)id->context;

    if (wc->opcode & IBV_WC_RECV) 
    {
        if (ctx->msg->id == MSG_MR) 
        {
            ctx->peer_addr = ctx->msg->data.mr.addr;
            ctx->peer_rkey = ctx->msg->data.mr.rkey;

            printf("received MR, sending file name\n");
            send_file_name(id);
        } 
        else if (ctx->msg->id == MSG_READY) 
        {
            printf("received READY, sending chunk\n");
            send_next_chunk(id);
        } 
        else if (ctx->msg->id == MSG_DONE) 
        {
            printf("received DONE, disconnecting\n");
            rc_disconnect(id);
            return;
        }

        post_receive(id);
    }
}

// ./client 192.168.40.4 test
int main(int argc, char **argv)
{
    struct client_context ctx;

    if (argc != 3) 
    {
        fprintf(stderr, "usage: %s <server-address> <file-name>\n", argv[0]);
        return 1;
    }

    ctx.file_name = basename(argv[2]);
    ctx.fd = open(argv[2], O_RDONLY);

    if (ctx.fd == -1) 
    {
        fprintf(stderr, "unable to open input file \"%s\"\n", ctx.file_name);
        return 1;
    }


    rc_client_loop(argv[1], DEFAULT_PORT, &ctx);

    close(ctx.fd);

    return 0;
}