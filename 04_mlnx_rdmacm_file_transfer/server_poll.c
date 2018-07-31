#include <fcntl.h>
#include <sys/stat.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <event2/event.h>
#include <event2/thread.h>
#include <event2/event-config.h>

#include <rdma/rdma_cma.h>
#define MAX_FILE_NAME 256
const int TIMEOUT_IN_MS = 500;

struct context 
{
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    
};

struct each_qp_context
{
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

};
static struct context *s_ctx = NULL;

const char *DEFAULT_PORT = "12345";
const size_t BUFFER_SIZE = 10 * 1024 * 1024;

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


struct conn_context
{
    char *buffer;
    struct ibv_mr *buffer_mr;

    // message 留给 rdma read/write 交流使用
    struct message *msg;
    struct ibv_mr *msg_mr;

    int fd;
    char file_name[MAX_FILE_NAME];
};


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

static void on_completion(struct ibv_wc *wc);



void poll_cq(evutil_socket_t sock, short flags, void * arg)
{
    struct ibv_comp_channel* channel = (struct ibv_comp_channel*)arg;

    void *ctx = NULL;
    struct ibv_cq *cq;
    struct ibv_wc wc;
    int ret = 0 ;
    // while (1) 
    {
        ret = ibv_get_cq_event(channel, &cq, &ctx);
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

}

struct event_base*  channel_base = NULL;
void* thread_epoll_ibv_comp_channel(void* arg)
{
    struct ibv_comp_channel* channel = (struct ibv_comp_channel*)arg;
    
    channel_base = event_base_new(); 
    

    struct event*  tmp_event = event_new(channel_base, channel->fd, EV_READ | EV_PERSIST, poll_cq, (void*)(arg)) ;

    event_add(tmp_event ,0); 

    
    event_base_dispatch(channel_base);
    

}

void add_to_comp_channel_epoll(void* arg)
{
    struct ibv_comp_channel* channel = (struct ibv_comp_channel*)arg;
    struct event*  tmp_event = event_new(channel_base, channel->fd, EV_READ | EV_PERSIST, poll_cq, (void*)(arg)) ;
    event_add(tmp_event ,0); 
}

void CreateReconnectThread(struct ibv_comp_channel* tmp_channel)
{
    int res;
    pthread_t epoll_ibv_comp_channel_thead = 0;
    if((res=pthread_create(&epoll_ibv_comp_channel_thead,NULL,thread_epoll_ibv_comp_channel, (void*)tmp_channel ))!=0)
    {
        fprintf(stderr,"poll_cq ibv_get_cq_event error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    if( (res = pthread_detach(epoll_ibv_comp_channel_thead) ) != 0 )
    {
        fprintf(stderr,"poll_cq ibv_get_cq_event error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
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
        fprintf(stderr,"ibv_alloc_pd error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    

    // ret = pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL);
    // if (0 != ret)
    // {
    //     rc_die("cannot pthread_create to poll_cq\n");
    // }
}

void build_params(struct rdma_conn_param *params)
{
    memset(params, 0, sizeof(*params));

    params->initiator_depth = 1;
    params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct each_qp_context *tmp_qp_context, struct ibv_qp_init_attr *qp_attr)
{
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = tmp_qp_context->cq;
    qp_attr->recv_cq = tmp_qp_context->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void build_each_qp_context(struct each_qp_context **tmp_qp_context)
{
    (*tmp_qp_context) = (struct each_qp_context *)malloc(sizeof(struct each_qp_context));
    (*tmp_qp_context)->comp_channel = ibv_create_comp_channel(s_ctx->ctx);
    if (NULL == (*tmp_qp_context)->comp_channel )
    {
        fprintf(stderr,"ibv_create_comp_channel error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    (*tmp_qp_context)->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, (*tmp_qp_context)->comp_channel, 0); /* cqe=10 is arbitrary */
    if (NULL == (*tmp_qp_context)->cq)
    {
        fprintf(stderr,"ibv_create_cq error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    int ret = 0;
    ret = ibv_req_notify_cq((*tmp_qp_context)->cq, 0);
    if (0 != ret)
    {
        fprintf(stderr,"ibv_req_notify_cq error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }

    static int first_build_qp = 1;
    if (1 == first_build_qp)//第一次创建cc，cq，则创建epoll后台线程
    {
        CreateReconnectThread((*tmp_qp_context)->comp_channel);
    }
    else//否则将新的cc加入到epoll中
    {
        add_to_comp_channel_epoll((*tmp_qp_context)->comp_channel);
    }
    first_build_qp =  0;

}

void build_connection(struct rdma_cm_id *id)
{
    struct each_qp_context *tmp_qp = NULL;


    struct ibv_qp_init_attr qp_attr;

    fprintf(stderr, "id->verbs = %p\n",id->verbs );// always same
    build_context(id->verbs);

    build_each_qp_context(&tmp_qp);
    
    build_qp_attr(tmp_qp,&qp_attr);



    int ret = 0; 
    ret = rdma_create_qp(id, s_ctx->pd, &qp_attr);
    if (0 != ret )
    {
        fprintf(stderr,"rdma_create_qp error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
}
static void send_message(struct rdma_cm_id *id)
{
    struct conn_context *ctx = (struct conn_context *)id->context;

    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)id;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t)ctx->msg;
    sge.length = sizeof(*ctx->msg);
    sge.lkey = ctx->msg_mr->lkey;

    int ret = 0 ;
    ibv_post_send(id->qp, &wr, &bad_wr);
    if (0 != ret )
    {
        fprintf(stderr,"ibv_post_send error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
}

static void post_receive(struct rdma_cm_id *id)
{
    struct ibv_recv_wr wr, *bad_wr = NULL;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t)id;
    wr.sg_list = NULL;
    wr.num_sge = 0;

    int ret = 0 ;
    ret = ibv_post_recv(id->qp, &wr, &bad_wr);
    if (0 != ret )
    {
        fprintf(stderr,"ibv_post_recv error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
}

static void on_pre_conn(struct rdma_cm_id *id)
{
    struct conn_context *ctx = (struct conn_context *)malloc(sizeof(struct conn_context));

    id->context = ctx;

    ctx->file_name[0] = '\0'; // take this to mean we don't have the file name

    posix_memalign((void **)&ctx->buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
    ctx->buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if ( NULL == ctx->buffer_mr )
    {
        rc_die("ctx->buffer_mr ibv_reg_mr() error\n");
    }

    posix_memalign((void **)&ctx->msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->msg));
    ctx->msg_mr = ibv_reg_mr(rc_get_pd(), ctx->msg, sizeof(*ctx->msg), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (NULL == ctx->msg_mr)
    {
        rc_die("ctx->msg_mr ibv_reg_mr() error");
    }
    post_receive(id);
}

static void on_connection(struct rdma_cm_id *id)
{
    struct conn_context *ctx = (struct conn_context *)id->context;

    ctx->msg->id = MSG_MR;
    ctx->msg->data.mr.addr = (uintptr_t)ctx->buffer_mr->addr;
    ctx->msg->data.mr.rkey = ctx->buffer_mr->rkey;

    send_message(id);
}

static void on_completion(struct ibv_wc *wc)
{
    struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
    struct conn_context *ctx = (struct conn_context *)id->context;

    if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) 
    {
        uint32_t size = ntohl(wc->imm_data);

        if (size == 0) 
        {
            ctx->msg->id = MSG_DONE;
            send_message(id);
            // don't need post_receive() since we're done with this connection
        } 
        else if (ctx->file_name[0]) 
        {
            ssize_t ret;

            printf(" %lu received %i bytes.\n", pthread_self(),size);

            ret = write(ctx->fd, ctx->buffer, size);

            if (ret != size)
            {
                rc_die("write() failed");
            }

            post_receive(id);

            ctx->msg->id = MSG_READY;
            send_message(id);

        } 
        else 
        {
            memcpy(ctx->file_name, ctx->buffer, (size > MAX_FILE_NAME) ? MAX_FILE_NAME : size);
            ctx->file_name[size - 1] = '\0';

            printf("opening file %s\n", ctx->file_name);

            ctx->fd = open(ctx->file_name, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

            if (ctx->fd == -1)
            {
                rc_die("open() failed");
            }

            post_receive(id);

            ctx->msg->id = MSG_READY;
            send_message(id);
        }
    }
}

static void on_disconnect(struct rdma_cm_id *id)
{
    struct conn_context *ctx = (struct conn_context *)id->context;

    close(ctx->fd);

    ibv_dereg_mr(ctx->buffer_mr);
    ibv_dereg_mr(ctx->msg_mr);

    free(ctx->buffer);
    free(ctx->msg);

    printf("finished transferring %s\n", ctx->file_name);

    free(ctx);
}


void event_loop(struct rdma_event_channel *ec)
{
    struct rdma_cm_event *event = NULL;
    struct rdma_conn_param cm_params;

    build_params(&cm_params);

    int ret = 0;

    while (rdma_get_cm_event(ec, &event) == 0) 
    {
        struct rdma_cm_event event_copy;

        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);

        if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST) // server
        {
            fprintf(stderr,"event_copy.id: %p\n",event_copy.id);// change every time

            build_connection(event_copy.id);

            on_pre_conn(event_copy.id);

            ret = rdma_accept(event_copy.id, &cm_params);
            if (0 != ret )
            {
                fprintf(stderr,"event loog rdma_accept() error: %s\n",strerror(errno));
                exit(EXIT_FAILURE);
            }

        } 
        else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED) 
        {
            on_connection(event_copy.id);
        } 
        else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED) 
        {
            rdma_destroy_qp(event_copy.id);

            on_disconnect(event_copy.id);

            rdma_destroy_id(event_copy.id);

        } 
        else 
        {
            rc_die("unknown event\n");
        }
    }
}






void rc_server_loop(const char *port)
{
    struct sockaddr_in6 addr;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;

    memset(&addr, 0, sizeof(addr));
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(atoi(port));

    ec = rdma_create_event_channel();
    if (NULL == ec)
    {
        fprintf(stderr,"rdma_create_event_channel error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    else
    {
        fprintf(stderr, "%s\n","rdma_create_event_channel success" );
    }

    int ret = 0;
    ret = rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP);
    if (0 != ret )
    {
        fprintf(stderr,"rdma_create_id error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    else
    {
        fprintf(stderr, "%s\n","rdma_create_id success" );
    }


    ret = rdma_bind_addr(listener, (struct sockaddr *)&addr);
    if (0 != ret)
    {
        fprintf(stderr,"rdma_bind_addr error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    else
    {
        fprintf(stderr, "%s\n","rdma_bind_addr success" );
    }

    ret = rdma_listen(listener, 10); /* backlog=10 is arbitrary */
    if (0 != ret )
    {
        fprintf(stderr,"rdma_listen error: %s\n",strerror(errno));
        exit(EXIT_FAILURE);
    }
    else
    {
        fprintf(stderr, "%s\n","rdma_listen success" );
    }


    event_loop(ec);

    rdma_destroy_id(listener);
    rdma_destroy_event_channel(ec);
}




// ./server
int main(int argc, char **argv)
{
    printf("waiting for connections. interrupt (^C) to exit.\n");

    rc_server_loop(DEFAULT_PORT);

    return 0;
}
