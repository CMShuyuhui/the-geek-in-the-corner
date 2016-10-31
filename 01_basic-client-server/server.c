#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

// x not-zero or not-null , echo error
#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
// x null or zero , echo error
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const int BUFFER_SIZE = 1024;
/**
 * we postpone building the verbs context until re receive ur first connextion request because the rdmacm listener ID
 * isn't necessarily bound to a specific RDMA devic( and associated verbs context).
 * However , the first connect request we receive will have a valid verbs context structure at id->verbs.
 * Building the verbs context involves setting up a static context structure, creating a protextion domain, 
 * creating a completion queue, creating a completion channel, adn starting a thread to pull comoletions from the queue.
 */
struct context 
{
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

/**
 * when we receive a connection request , we first build our verbs context if it hasn't already been built.
 * Then, after building our connection context structure,
 * we pre-post our receives(more on the si a bit),and accept the connection request.
 */
struct connection 
{
  struct ibv_qp *qp;          // pointer to the queue pair(redundant, but simplifies the code lightly)

  struct ibv_mr *recv_mr;     // two buffers
  struct ibv_mr *send_mr;     

  char *recv_region;          // two memory regions
  char *send_region;          // memory used for sends/recieves has tobe "registered" with the verbs library
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
  #if _USE_IPV6
    struct sockaddr_in6 addr;
  #else
    struct sockaddr_in addr;
  #endif
    struct rdma_cm_event *event = NULL;
    struct rdma_cm_id *listener = NULL;
    struct rdma_event_channel *ec = NULL;
    uint16_t port = 0;

    memset(&addr, 0, sizeof(addr));
  #if _USE_IPV6
    addr.sin6_family = AF_INET6;
  #else
    addr.sin_family = AF_INET;
  #endif

  // create an event channel so thar we can receive rdmacm events,
  // such as connection-request adn connection-established notifications
  TEST_Z(ec = rdma_create_event_channel());

  //  Allocate a communication identifier.
  // Notes:
  // Rdma_cm_id's are conceptually equivalent to a socket for RDMA
  // communication.  The difference is that RDMA communication requires
  // explicitly binding to a specified RDMA device before communication
  // can occur, and most operations are asynchronous in nature.  Communication
  // events on an rdma_cm_id are reported through the associated event
  // channel.  Users must release the rdma_cm_id by calling rdma_destroy_id.
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));

  // bind the listener ID to an socket address
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));

  // wait for a connection request
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  // wait in a loop for events
  // event loop gets an event from rdmacm, acknowledges the event, then process it.
  while (rdma_get_cm_event(ec, &event) == 0) 
  {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    // process the event
    if (on_event(&event_copy))  
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  // creating a protection domain
  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  // creating a completion channel
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  // creating a completion queue
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  // the poller waits on the channel, acknowledges the completion, rearms the completion queue(with ibv_req_notify_cq()),
  // the pulls events from the queue until none are left
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
  // starting a thread to pull completions from the queue
  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq; //
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC; //qp_type is set to indicate we want a reliable, connection-oriented queue pair

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1; // scatter/gather element(SGE; effectively a memory location/size tuple) per send or receive request
  qp_attr->cap.max_recv_sge = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

// The reason it is necessary to post receive work requests(WRs) before accepting the connection is that
//  the underlying hardware won't buffer incoming message .
//  if a receive request has not been posted to the work queue, the incoming message is rejected and 
//  the peer will receive a receiver-not-ready(RNR) error.
void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;
  // the (arbitrary) wr_id field is used to store a connection context pointer.
  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_region = malloc(BUFFER_SIZE);
  conn->recv_region = malloc(BUFFER_SIZE);
  // register send_region and recv_region with verbs. local write and remote write access
  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->send_region,
    BUFFER_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->recv_region,
    BUFFER_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

void on_completion(struct ibv_wc *wc)
{
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV) {
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

    printf("received message: %s\n", conn->recv_region);

  } else if (wc->opcode == IBV_WC_SEND) {
    printf("send completed successfully.\n");
  }
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  printf("received connection request.\n");

  // when we receive a connection request ,first build the verbs context if it hasn't already been built
  build_context(id->verbs);
  // After building the verbs context, we have to initialize the queue pair attributes structure.
  build_qp_attr(&qp_attr);

  // create the queue pair
  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  conn->qp = id->qp;
  // allocate and register memory for our send and receive operations
  register_memory(conn);

  // The reason it is necessary to post receive work requests(WRs) before accepting the connection is that
  //  the underlying hardware won't buffer incoming message .
  //  if a receive request has not been posted to the work queue, the incoming message is rejected and 
  //  the peer will receive a receiver-not-ready(RNR) error.
  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));
  // ready to accept the connect request
  TEST_NZ(rdma_accept(id, &cm_params));
  return 0;
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  snprintf(conn->send_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());

  printf("connected. posting send...\n");

  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->send_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);

  free(conn);

  rdma_destroy_id(id);

  return 0;
}

// the event handle for the passive side of the connection is only interested in three events
int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

