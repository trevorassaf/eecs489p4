/*
 * Copyright (c) 2014, 2015 University of Michigan, Ann Arbor.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that the above copyright notice and this paragraph are
 * duplicated in all such forms and that any documentation,
 * advertising materials, and other materials related to such
 * distribution and use acknowledge that the software was developed
 * by the University of Michigan, Ann Arbor. The name of the University 
 * may not be used to endorse or promote products derived from this 
 * software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
 * WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Author: Sugih Jamin (jamin@eecs.umich.edu)
 *
*/
#include <stdio.h>         // fprintf(), perror(), fflush()
#include <stdlib.h>        // atoi(), random()
#include <assert.h>        // assert()
#include <limits.h>        // LONG_MAX, INT_MAX
#include <errno.h>         // errno
#include <iostream>
using namespace std;
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>      // socklen_t
#include "wingetopt.h"
#else
#include <string.h>        // memset(), memcmp(), strlen(), strcpy(), memcpy()
#include <unistd.h>        // getopt(), STDIN_FILENO, gethostname()
#include <signal.h>        // signal()
#include <netdb.h>         // gethostbyname(), gethostbyaddr()
#include <netinet/in.h>    // struct in_addr
#include <arpa/inet.h>     // htons(), inet_ntoa()
#include <sys/types.h>     // u_short
#include <sys/socket.h>    // socket API, setsockopt(), getsockname()
#include <sys/ioctl.h>     // ioctl(), FIONBIO
#include <sys/time.h>      // gettimeofday()
#endif
#ifdef __APPLE__
#include <OpenGL/gl.h>
#else
#include <GL/gl.h>
#endif

#include "ltga.h"
#include "socks.h"
#include "netimg.h"
#include "imgdb.h"

#define USECSPERSEC 1000000

/*
 * Flow::readimg: load TGA image from file "imgname" to Flow::curimg.
 * "imgname" must point to valid memory allocated by caller.
 * Terminate process on encountering any error.
 * Returns NETIMG_FOUND if "imgname" found, else returns NETIMG_NFOUND.
 */
char Flow::
readimg(char *imgname, int verbose)
{
  string pathname=IMGDB_FOLDER;

  if (!imgname || !imgname[0]) {
    return(NETIMG_ENAME);
  }
  
  curimg.LoadFromFile(pathname+IMGDB_DIRSEP+imgname);

  if (!curimg.IsLoaded()) {
    return(NETIMG_NFOUND);
  }

  if (verbose) {
    cerr << "Image: " << endl;
    cerr << "       Type = " << LImageTypeString[curimg.GetImageType()] 
         << " (" << curimg.GetImageType() << ")" << endl;
    cerr << "      Width = " << curimg.GetImageWidth() << endl;
    cerr << "     Height = " << curimg.GetImageHeight() << endl;
    cerr << "Pixel depth = " << curimg.GetPixelDepth() << endl;
    cerr << "Alpha depth = " << curimg.GetAlphaDepth() << endl;
    cerr << "RL encoding = " << (((int) curimg.GetImageType()) > 8) << endl;
    /* use curimg.GetPixels()  to obtain the pixel array */
  }
  
  return(NETIMG_FOUND);
}

/*
 * Flow::marshall_imsg: Initialize *imsg with image's specifics.
 * Upon return, the *imsg fields are in host-byte order.
 * Return value is the size of the image in bytes.
 *
 * Terminate process on encountering any error.
 */
double Flow::
marshall_imsg(imsg_t *imsg)
{
  int alpha, greyscale;

  imsg->im_depth = (unsigned char)(curimg.GetPixelDepth()/8);
  imsg->im_width = curimg.GetImageWidth();
  imsg->im_height = curimg.GetImageHeight();
  alpha = curimg.GetAlphaDepth();
  greyscale = curimg.GetImageType();
  greyscale = (greyscale == 3 || greyscale == 11);
  if (greyscale) {
    imsg->im_format = alpha ? GL_LUMINANCE_ALPHA : GL_LUMINANCE;
  } else {
    imsg->im_format = alpha ? GL_RGBA : GL_RGB;
  }

  return((double) (imsg->im_width*imsg->im_height*imsg->im_depth));
}

/*
 * Flow::init
 * initialize flow by:
 * - indicating that flow is "in_use"
 * - loading and initializing image by calling Flow::readimg()
 *   and Flow::marshall_imsg(), update imsg->im_type accordingly.
 *   Also initialize member variables "ip" and "snd_next"
 * - initialize "mss" and "datasize", ensure that socket send
 *   buffer is at least mss size
 * - set flow's reserved rate "frate" to client's specification
 * - initial flow finish time is current global minimum finish time
 * - populate a struct msghdr for sending chunks of image
 * - save current system time as flow start time.  For gated start,
 *   this may be updated later with actual start time.
 * Assume that all fields in *iqry are already in host-byte order.
 * Leave all fields in *imsg in host-byte order also.
*/
void Flow::
init(int sd, struct sockaddr_in *qhost, iqry_t *iqry,
     imsg_t *imsg, float currFi)
{
  int err, usable;
  socklen_t optlen;
  double imgdsize;

  imsg->im_type = readimg(iqry->iq_name, 1);
  
  if (imsg->im_type == NETIMG_FOUND) {

    // flow is in use
    in_use = 1;
    
    // initialize imsg
    imgdsize = marshall_imsg(imsg);
    net_assert((imgdsize > (double) LONG_MAX),
               "imgdb::sendimg: image too big");
    imgsize = (long)imgdsize;

    // ip points to the start of byte buffer holding image
    ip = (char *) curimg.GetPixels();
    snd_next = 0;

    mss = iqry->iq_mss;
    /* make sure that the send buffer is of size at least mss. */
    optlen = sizeof(int);
    err = getsockopt(sd, SOL_SOCKET, SO_SNDBUF, &usable, &optlen);
    net_assert(err < 0, "imgdb::sendimg: getsockopt SNDBUF");
    if (usable < (int) mss) {
      usable = (int) mss;
      err = setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &usable, sizeof(int));
      net_assert((err < 0), "Flow::init: setsockopt SNDBUF");
    }
    datasize = mss - sizeof(ihdr_t) - NETIMG_UDPIP;

    // flow's reserved rate as specified by client
    // flow's initial finish time is the current global minimum finish time
    frate = iqry->iq_frate;
    Fi = currFi;
    
    /* 
     * Populate a struct msghdr with information of the destination client,
     * a pointer to a struct iovec array.  The iovec array should be of size
     * NETIMG_NUMIOV.  The first entry of the iovec should be initialized
     * to point to an ihdr_t, which should be re-used for each chunk of data
     * to be sent.
     */
    client = *qhost;
    msg.msg_name = &client;
    msg.msg_namelen = sizeof(sockaddr_in);
    msg.msg_iov = iov;
    msg.msg_iovlen = NETIMG_NUMIOV;
    msg.msg_control = NULL;
    msg.msg_controllen = 0;
    msg.msg_flags = 0;
    
    hdr.ih_vers = NETIMG_VERS;
    hdr.ih_type = NETIMG_DATA;
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(ihdr_t);
    
    /* for non-gated flow starts */
    gettimeofday(&start, NULL);
  }

  return;
}

/*
 * Flow::nextFi: compute the flow's next finish time
 * from the size of the current segment, the flow's
 * reserved rate, and the multiplier passed in.
 * The multiplier is linkrate/total_reserved_rate.
 * To avoid unnecessary arithmetic, you can assume that
 * the Fi's are multiplied by 128 (or 1024/8) so you
 * can keep the segment size in bytes instead of Kbits.
 * Since we're comparing relative finish times (Fi's),
 * the unit doesn't really matter, as long as the ordering
 * is correct.
*/
float Flow::
nextFi(float multiplier)
{
  /* size of this segment */
  segsize = imgsize - snd_next;
  segsize = segsize > datasize ? datasize : segsize;

  /* Task 2: YOUR CODE HERE */
  /* Replace the following return statement with your
     computation of the next finish time as indicated above
     and return the result instead. */
  /* DONE */
  //float segment_delay = (1.0 * segsize) / (multiplier * frate) / 128.0 * 1E6;
  float segment_delay = (1.0 * segsize) / (multiplier * frate);
   // fprintf(stderr, "WFQ computation: segsize: %u, frate: %d, multiplier: %f, curr-fi: %f, segment-delay: %f\n",
   //     segsize, frate, multiplier, Fi, segment_delay);
  
  return Fi + segment_delay;
}

/*
 * Flow::sendpkt:
 * Send the image contained in *image to the client
 * pointed to by *client. Send the image in
 * chunks of segsize, not to exceed mss, instead of
 * as one single image.
 * The argument "sd" is the socket to send packet out of.
 * The argument "fd" is the array index this flow occupies
 * on the flow table.  It is passed in here just so that we can
 * log it with the packet transmission message.
 * Update the flow's finish time to the current global
 * minimum finish time passed in as "currFi".
 *
 * Return 0 if there's more of the image to send.
 * Return 1 if we've finished sending image.
 * Terminate process upon encountering any error.
*/
int Flow::
sendpkt(int sd, int fd, float currFi)
{

  int bytes;

  // update the flow's finish time to the current
  // global minimum finish time
  Fi = currFi;

  /* 
   * Send one segment of data of size segsize at each iteration.
   * Point the second entry of the iovec to the correct offset
   * from the start of the image.  Update the sequence number
   * and size fields of the ihdr_t header to reflect the byte
   * offset and size of the current chunk of data.  Send
   * the segment off by calling sendmsg().
   */
  iov[1].iov_base = ip+snd_next;
  iov[1].iov_len = segsize;
  hdr.ih_seqn = htonl(snd_next);
  hdr.ih_size = htons(segsize);
  
  bytes = sendmsg(sd, &msg, 0);
  net_assert((bytes < 0), "imgdb_sendimage: sendmsg");
  net_assert((bytes != (int)(segsize+sizeof(ihdr_t))), "Flow::sendpkt: sendmsg bytes");
  
  fprintf(stderr, "WFQ::sendpkt() flow %d: sent offset 0x%x, Fi: %.6f, %d bytes\n",
          fd, snd_next, Fi, segsize);
  snd_next += segsize;
  
  if ((int) snd_next < imgsize) {
    return 0;
  } else {
    return 1;
  }
}

/*
 * imgdb_args: parses command line args.
 *
 * Returns 0 on success or 1 on failure.
 *
 * Nothing else is modified.
 */
int imgdb::
args(int argc, char *argv[])
{

  client1FlowPercentage = 0.5;

  char c;
  extern char *optarg;
  int arg;

  if (argc < 1) {
    return (1);
  }

  linkrate = IMGDB_LRATE;
  minflow = IMGDB_MINFLOW;

  while ((c = getopt(argc, argv, "l:g:f:")) != EOF) {
    switch (c) {
    case 'l':
      arg = atoi(optarg);
      if (arg < IMGDB_MINLRATE || arg > IMGDB_MAXLRATE) {
        return(1);
      }
      linkrate = (unsigned short) arg*1024;  // in Kbps
      break;
    case 'g':
      arg = atoi(optarg);
      if ((arg < IMGDB_MINFLOW || arg > IMGDB_MAXFLOW) && arg != 1) {
        return(1);
      }
      minflow = (short) arg;
      break;
    case 'f':
      client1FlowPercentage = atof(optarg);
      if (client1FlowPercentage > 1 || client1FlowPercentage < 0) {
        return 1;
      }
      break;
    default:
      return(1);
      break;
    }
  }

  wfqLinkRate = static_cast<unsigned short>(client1FlowPercentage * linkrate);
  fifoLinkRate = linkrate - wfqLinkRate;

  return (0);
}

/*
 * imgdb: default constructor
*/
imgdb::
imgdb(int argc, char *argv[])
{ 
  hasFifo = false;
  started=0; nflow=0; rsvdrate=0; currFi=0.0; 

  sd = socks_servinit((char *) "imgdb", &self, sname);

  // parse args, see the comments for imgdb::args()
  if (args(argc, argv)) {
    fprintf(stderr, "Usage: %s [ -l <linkrate [1, 10 Mbps]> -g <minflow> ]\n", argv[0]); 
    exit(1);
  }
  
  srandom(NETIMG_SEED+linkrate+minflow);
}

/* 
 * recvqry: receives an iqry_t packet and stores the client's address
 * and port number in the qhost variable.  Checks that
 * the incoming iqry_t packet is of version NETIMG_VERS and of type
 * NETIMG_SYNQRY.
 *
 * If error encountered when receiving packet or if packet is of the
 * wrong version or type returns appropriate NETIMG error code.  The
 * receive is done non-blocking.  If the socket buffer is empty,
 * return NETIMG_EAGAIN. Otherwise returns 0.
 *
 * Nothing else is modified.
*/
char imgdb::
recvqry(int sd, struct sockaddr_in *qhost, iqry_t *iqry)
{
  int bytes;  // stores the return value of recvfrom()

  /*
   * Call recvfrom() to receive the iqry_t packet from
   * qhost.  Store the client's address and port number in
   * qhost and store the return value of
   * recvfrom() in local variable "bytes".
  */
  socklen_t len;
  
  len = sizeof(struct sockaddr_in);
  bytes = recvfrom(sd, iqry, sizeof(iqry_t), started ? MSG_DONTWAIT: 0,
                   (struct sockaddr *) qhost, &len);
  if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    return (NETIMG_EAGAIN);
  }
  
  if (bytes != sizeof(iqry_t)) {
    return (NETIMG_ESIZE);
  }
  if (iqry->iq_vers != NETIMG_VERS) {
    return(NETIMG_EVERS);
  }
  if (iqry->iq_type != NETIMG_SYNQRY) {
    return(NETIMG_ETYPE);
  }
  if (strlen((char *) iqry->iq_name) >= NETIMG_MAXFNAME) {
    return(NETIMG_ENAME);
  }

  return(0);
}

/* imgdb::sendimsg:
 *
 * Prepares imsg for transmission: fills in im_vers and converts
 * integers to network byte order before transmission.  Note that
 * im_type is set by the caller and should not be modified.  Sends the
 * imsg packet to qhost using sendto().  Returns the return
 * value of sendto().  Nothing else is modified.
*/
void imgdb::
sendimsg(int sd, struct sockaddr_in *qhost, imsg_t *imsg)
{
  int bytes;

  imsg->im_vers = NETIMG_VERS;
  imsg->im_width = htons(imsg->im_width);
  imsg->im_height = htons(imsg->im_height);
  imsg->im_format = htons(imsg->im_format);

  // send the imsg packet to client
  bytes = sendto(sd, (char *) imsg, sizeof(imsg_t), 0, (struct sockaddr *) qhost,
                 sizeof(struct sockaddr_in));
  net_assert((bytes != sizeof(imsg_t)), "imgdb::sendimsg: sendto");

  return;
}

/*
 * imgdb::handleqry
 * Check for an iqry_t packet from client and set up a flow
 * if an iqry_t packet arrives.
 *
 * Once a flow arrives, look for the first empty slot in flow[] to
 * hold the new flow.  If an empty slot exists, check that we haven't
 * hit linkrate capacity.  We can only add a flow if there's enough
 * linkrate left over to accommodate the flow's reserved rate.  If a flow
 * cannot be admitted due to capacity limit, return an imsg_t packet
 * with im_type set to NETIMG_EFULL.
 *
 * Once a flow is admitted, increment flow count and total reserved
 * rate, then call Flow::init() to initialize the flow.  If the
 * queried image is found, Flow::init(), would update the imsg
 * response packet accordingly.
 * 
 * If minflow number of flows have arrived or total reserved rate is
 * at link capacity, toggle the "started" member variable to on (1)
 * and reset the start time of each flow to the current wall clock
 * time.
 * 
 * Finally send back the imsg_t response packet.
*/
int imgdb::
handleqry()
{
  int i;
  iqry_t iqry;
  imsg_t imsg;
  struct sockaddr_in qhost;
  
  imsg.im_type = recvqry(sd, &qhost, &iqry);
  if (!imsg.im_type) {
   
    bool is_qry_fifo_req = false;
    iqry.iq_mss = (unsigned short) ntohs(iqry.iq_mss);
    
    if (iqry.iq_frate) {
      iqry.iq_frate = (unsigned short) ntohs(iqry.iq_frate); 
    } else {
      // Client 2 linkrate is the fraction of the linkrate not assigned to Client 1
    
      // Check for registered FIFO client
      if (hasFifo) {
        fprintf(stderr, "FIFO::handleqry: flow already registered!\n");

        imsg_t imsg;
        imsg.im_vers = NETIMG_VERS;
        imsg.im_type = NETIMG_EFULL;
       
        int bytes = sendto(sd, (char *) &imsg, sizeof(imsg_t), 0, (struct sockaddr *) &qhost, sizeof(struct sockaddr_in)); 
        net_assert((bytes != sizeof(imsg_t)), "imgdb::handleqry: sendto");
        return 1;
      }
     
      if (nflow >= IMGDB_MAXFLOW) {
        fprintf(stderr, "FIFO::handleqry: insufficient open flow slots!\n");
        
        imsg_t imsg;
        imsg.im_vers = NETIMG_VERS;
        imsg.im_type = NETIMG_EFULL;
       
        int bytes = sendto(sd, (char *) &imsg, sizeof(imsg_t), 0, (struct sockaddr *) &qhost, sizeof(struct sockaddr_in)); 
        net_assert((bytes != sizeof(imsg_t)), "imgdb::handleqry: sendto");
        return 1;
      }

      hasFifo = true;
      is_qry_fifo_req = true;

      // Initialize FIFO variables
      fifoDatasize = iqry.iq_mss - sizeof(ihdr_t) - NETIMG_UDPIP;
      fifoBsize = (float) fifoDatasize / IMGDB_BPTOK;
      
      if (iqry.iq_rwnd) {
        fifoBsize *= iqry.iq_rwnd;
      }
      
      fifoTokensCreated = fifoBsize;

      fifoTrate = 0x80 * fifoLinkRate / IMGDB_BPTOK;
      fifoIqry = iqry;
    
      fifoImgsize = 0;
      fifoSendNext = 0;

      fifoCurrFi = 0;

      ++nflow;

      if (nflow >= minflow) {
        assert(!started); 
        started = true;
      }

      // Initialize image flow
      string pathname = IMGDB_FOLDER;
      string imgname = iqry.iq_name;
      string full_imgname = pathname+IMGDB_DIRSEP+imgname;

      if (!iqry.iq_name[0]) {
        NETIMG_ENAME;
      }

      fifoImg.LoadFromFile(full_imgname);

      if (!fifoImg.IsLoaded()) {
        return NETIMG_NFOUND;
      }
      
      cerr << "Image: " << endl;
      cerr << "       Type = " << LImageTypeString[fifoImg.GetImageType()] 
           << " (" << fifoImg.GetImageType() << ")" << endl;
      cerr << "      Width = " << fifoImg.GetImageWidth() << endl;
      cerr << "     Height = " << fifoImg.GetImageHeight() << endl;
      cerr << "Pixel depth = " << fifoImg.GetPixelDepth() << endl;
      cerr << "Alpha depth = " << fifoImg.GetAlphaDepth() << endl;
      cerr << "RL encoding = " << (((int) fifoImg.GetImageType()) > 8) << endl;
      /* use curimg.GetPixels()  to obtain the pixel array */
      
      fprintf(stderr, "FIFO::handleqry: flow added\n");

      // Initialize channel
      fifoImsg.im_vers = NETIMG_VERS;
      fifoImsg.im_type = NETIMG_FOUND;
      fifoImsg.im_depth = (unsigned char) (fifoImg.GetPixelDepth()/8);
      fifoImsg.im_width = htons(fifoImg.GetImageWidth());
      fifoImsg.im_height = htons(fifoImg.GetImageHeight());
      int fifo_alpha = fifoImg.GetAlphaDepth();
      int fifo_grayscale = fifoImg.GetImageType();
      fifo_grayscale = (fifo_grayscale == 3 || fifo_grayscale == 11);
      
      if (fifo_grayscale) {
        fifoImsg.im_format = fifo_alpha ? GL_LUMINANCE_ALPHA : GL_LUMINANCE;
      } else {
        fifoImsg.im_format = fifo_alpha ? GL_RGBA : GL_RGB; 
      }

      fifoImsg.im_format = htons(fifoImsg.im_format);

      fifoImgsize = ntohs(fifoImsg.im_width) * ntohs(fifoImsg.im_height) * fifoImsg.im_depth;

      fifoClient = qhost;

      // Send header
      int bytes = sendto(
          sd,
          (char *) &fifoImsg,
          sizeof(imsg_t),
          0,
          (struct sockaddr *) &fifoClient,
          sizeof(struct sockaddr_in)
      );

      net_assert(bytes != sizeof(imsg_t), "failed to send imsg header");
      fifoMss = iqry.iq_mss;
      fifoRwnd = iqry.iq_rwnd;

      // Resize send buffer
      unsigned int fifo_optlen = sizeof(int);
     
      int fifo_usable = -1;
      int fifo_offered = fifoMss;
      int fifo_err = getsockopt(
          sd, SOL_SOCKET, SO_SNDBUF, &fifo_usable, &fifo_optlen);
      net_assert(fifo_err < 0, "imgdb::sendimg: getsockopt SNDBUF");
      if (fifo_usable < fifo_offered) {
        fifo_err = setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &fifo_offered, sizeof(int));
        net_assert((fifo_err < 0), "imgdb::sendimg: setsockopt SNDBUF");
      }

      fifoMsg.msg_name = &fifoClient;
      fifoMsg.msg_namelen = sizeof(sockaddr_in);
      fifoMsg.msg_iov = fifoIov;
      fifoMsg.msg_iovlen = NETIMG_NUMIOV;
      fifoMsg.msg_control = 0;
      fifoMsg.msg_controllen = 0;
      fifoMsg.msg_flags = 0;

      fifoHdr.ih_vers = NETIMG_VERS;
      fifoHdr.ih_type = NETIMG_DATA;
      fifoIov[0].iov_base = &fifoHdr;
      fifoIov[0].iov_len = sizeof(ihdr_t);

      return 0;
    }
    
    /* 
     * Task 1: look for the first empty slot in flow[] to hold the new
     * flow.  If an empty slot exists, check that we haven't hit
     * linkrate capacity.  We can only add a flow if there's enough
     * linkrate left over to accommodate the flow's reserved rate.
     * Once a flow is admitted, increment flow count and total
     * reserved rate, then call Flow::init() to initialize the flow.
     * Flow::init() will update the imsg response packet accordingly.
     * If a flow cannot be admitted due to capacity limit, return an
     * imsg_t packet with im_type set to NETIMG_EFULL.
     */
    /* Task 1: YOUR CODE HERE */
    bool available_flow = false;
    unsigned int available_flow_idx = -1;
    
    if (nflow >= IMGDB_MAXFLOW) {
      fprintf(stderr, "imgdb::handleqry() Not enough open flow slots!\n");
      
      imsg_t imsg;
      imsg.im_vers = NETIMG_VERS;
      imsg.im_type = NETIMG_EFULL;
     
      int bytes = sendto(sd, (char *) &imsg, sizeof(imsg_t), 0, (struct sockaddr *) &qhost, sizeof(struct sockaddr_in)); 
      net_assert((bytes != sizeof(imsg_t)), "imgdb::handleqry: sendto");
      return 1;
    }

    if (rsvdrate + iqry.iq_frate >= linkrate && !is_qry_fifo_req) {
      fprintf(stderr, "imgdb::handleqry() Not enough linkrate available!\n");
      
      imsg_t imsg;
      imsg.im_vers = NETIMG_VERS;
      imsg.im_type = NETIMG_EFULL;
     
      int bytes = sendto(sd, (char *) &imsg, sizeof(imsg_t), 0, (struct sockaddr *) &qhost, sizeof(struct sockaddr_in)); 
      net_assert((bytes != sizeof(imsg_t)), "imgdb::handleqry: sendto");
      return 1;
    }

    for (i = 0; i < IMGDB_MAXFLOW; ++i) {
      if (!flow[i].in_use) {
        if (!available_flow) {
          available_flow_idx = i;
        }
        available_flow = true;
      } 
    }

    if (available_flow) {
      // Register new link
      ++nflow;
      if (!is_qry_fifo_req) {
        rsvdrate += iqry.iq_frate;
      }
      
      fprintf(
          stderr,
          "WFQ::handleqry: flow %u added, flow rate: %u, reserved link rate: %u\n",
          available_flow_idx, iqry.iq_frate, rsvdrate
      );

      flow[available_flow_idx].init(sd, &qhost, &iqry, &imsg, currFi);
    }

    /* Toggle the "started" member variable to on (1) if minflow number
     * of flows have arrived or total reserved rate is at link capacity
     * and set the start time of each flow to the current wall clock time.
     */
    assert(nflow <= minflow);
    if (!started && nflow == minflow) {
      started = 1;
      for (i = 0; i < IMGDB_MAXFLOW; ++i) {
        if (flow[i].in_use) {
          gettimeofday(&flow[i].start, NULL);
        }
      }
    }
  }
 
  if (imsg.im_type != NETIMG_EAGAIN) {
    // inform qhost of error or image dimensions if no error.
    sendimsg(sd, &qhost, &imsg);
    return(1);
  }

  return(0);
}


void imgdb::
sendpkt() {

  struct timeval end;
  int secs, usecs;

  float mult = 1.0 * wfqLinkRate / rsvdrate;
  
  unsigned int min_finish_time_idx = -1; 
  currFi = -1;

  // Find min finish time of WFQ flows 
  for (unsigned int i = 0; i < IMGDB_MAXFLOW; ++i) {
    if (flow[i].in_use) {
      float finish_time = flow[i].nextFi(mult);
//      fprintf(stderr, "- flow[%u] finish time: %f\n", i, finish_time);
      if (min_finish_time_idx == -1 || finish_time < currFi) {
        min_finish_time_idx = i;
        currFi = finish_time;
      }
    }
  }

  if (min_finish_time_idx != -1) {
    assert(min_finish_time_idx >= 0);

    // Compute delay, if we don't already have one
    if (!wfqLoaded) {
      assert(wfqDelayUsecs == 0);
      Flow& wfq_flow = flow[min_finish_time_idx];
      // float wfq_rate_bps = (1024.0 * wfq_flow.frate) / rsvdrate * wfqLinkRate;
      float wfq_rate_bps = 1024.0 * wfqLinkRate;
      float wfq_fin_usecs = (8.0 * wfq_flow.segsize) / wfq_rate_bps * 1E6;
      wfqDelayUsecs = wfq_fin_usecs;

      wfqLoaded = true;
    }
  } else {
    assert(hasFifo);
  }

  // Process next fifo packet
  if (hasFifo) {
    unsigned long int fifo_left = fifoImgsize - fifoSendNext;
    fifoSegsize = fifoDatasize > fifo_left
        ? fifo_left
        : fifoDatasize;

    // Load fifo next fifo packet, if need be
    if (!fifoLoaded) {
      assert(fifoDelayUsecs == 0);
      
      float fifo_fin_usecs = (8.0 * fifoSegsize) / (1024.0 * fifoLinkRate) * 1E6;
      fifoDelayUsecs = fifo_fin_usecs; 

      fifoLoaded = true;

      // Check if we need to accumulate tokens
      float fifo_segment_tokens = (float) fifoSegsize / IMGDB_BPTOK;
      if (fifoTokensCreated < fifo_segment_tokens) {
        
        float fifo_tokens_spawned = (float) random() / INT_MAX * fifoBsize + (fifo_segment_tokens - fifoTokensCreated); 
        float fifo_token_accum_usecs = fifo_tokens_spawned / fifoTrate * 1E6;

        fprintf(
            stderr,
            "FIFO::nextxmission() accumulating tokens for (s:ms:us) %u:%u:%u\n",
            (unsigned int) fifo_token_accum_usecs / 1000000,
            ((unsigned int) fifo_token_accum_usecs / 1000) % 1000,
            (unsigned int) fifo_token_accum_usecs % 1000
        );

        fifoDelayUsecs += fifo_token_accum_usecs;
        fifoTokensCreated += fifo_tokens_spawned;
      }
    }
  }

  bool send_fifo = false;

  // Determine which client to send
  if (min_finish_time_idx == -1) {
    send_fifo = true;
  } else {

    // fprintf(
    //     stderr,
    //     "COMPARE: fifoDelayUsecs: %f, wfqDelayUsecs: %f\n",
    //     fifoDelayUsecs,
    //     wfqDelayUsecs
    // );

    send_fifo = hasFifo && fifoDelayUsecs <= wfqDelayUsecs; 
  }

  // Send client packet
  if (send_fifo) {
    // Sleep until we can send
    usleep(fifoDelayUsecs);

    // Send fifo segment
    fifoIov[1].iov_base = fifoImg.GetPixels() + fifoSendNext;
    fifoIov[1].iov_len = fifoSegsize;
    fifoHdr.ih_seqn = htonl(fifoSendNext);
    fifoHdr.ih_size = htons(fifoSegsize);

    int fifo_bytes_sent = sendmsg(sd, &fifoMsg, 0);
   
    net_assert((fifo_bytes_sent < 0), "imgdb::sendimg: sendmsg for fifo failed");
    net_assert((fifo_bytes_sent != (int)(fifoSegsize + sizeof(ihdr_t))), "imgdb::sendimg: sendmsg bytes");

    // Discard used tokens 
    assert(fifoTokensCreated >= 0);
    float fifo_segment_tokens = (float) fifoSegsize / IMGDB_BPTOK;
    assert(fifo_segment_tokens <= fifoTokensCreated);
    fifoTokensCreated = (fifo_segment_tokens < fifoTokensCreated)
        ? fifoTokensCreated - fifo_segment_tokens
        : 0;

    fprintf(
        stderr,
        "FIFO::sendpkt() flow %d sent offset 0x%x, Fi: %.6f, %d bytes\n",
        -1,
        fifoSendNext,
        0.0,
        fifoSegsize
    );

    // Prepare to send next segment
    fifoSendNext += fifoSegsize;
    if (min_finish_time_idx != -1) {
      wfqDelayUsecs = (fifoDelayUsecs < wfqDelayUsecs)
          ? wfqDelayUsecs - fifoDelayUsecs
          : 0;
    }

    fifoDelayUsecs = 0;
    fifoLoaded = false;
    
    // Check if we're finished with the FIFO flow
    if (fifoSendNext >= fifoImgsize) {
      --nflow; 
      
      assert(nflow >= 0);
      if (nflow == 0) {
        started = 0;   
      }
      
      struct timeval fifo_end;
      gettimeofday(&fifo_end, NULL);
      
      usecs = USECSPERSEC - start.tv_usec + fifo_end.tv_usec;
      secs = fifo_end.tv_sec - start.tv_sec - 1;
      
      if (usecs > USECSPERSEC) {
        secs++;
        usecs -= USECSPERSEC;
      }
      
      fprintf(
          stderr,
          "FIFO::sendpkt() flow %d done, elapsed time (m:s:ms:us): %d:%d:%d:%d\n",
          -1,
          secs/60,
          secs%60,
          usecs/1000,
          usecs%1000
      );

      // Prepare for the next FIFO flow
      hasFifo = false;
      fifoSendNext = 0;
      start.tv_usec = 0;
    }

  } else { /* send wfq packet */

    // Sleep until we can send wfq packet
    assert(wfqDelayUsecs >= 0);
    usleep(wfqDelayUsecs);

    // Send wfq packet segment
    bool done = flow[min_finish_time_idx].sendpkt(sd, min_finish_time_idx, currFi);

    if (hasFifo) {
      fifoDelayUsecs = (wfqDelayUsecs < fifoDelayUsecs)
          ? fifoDelayUsecs - wfqDelayUsecs
          : 0;
    }

    wfqDelayUsecs = 0;
    wfqLoaded = false;

    if (done) {
      unsigned short flow_frate = flow[min_finish_time_idx].done();
      
      rsvdrate -= flow_frate;
      --nflow;

      if (nflow <= 0) {
        started = 0;
      }

      gettimeofday(&end, NULL);

      /* compute elapsed time */
      usecs = USECSPERSEC-start.tv_usec+end.tv_usec;
      secs = end.tv_sec - start.tv_sec - 1;
      if (usecs > USECSPERSEC) {
        secs++;
        usecs -= USECSPERSEC;
      }
      
      fprintf(stderr,
          "WFQ::sendpkt() flow %d done, elapsed time (m:s:ms:us): %d:%d:%d:%d\n",
          min_finish_time_idx,
          secs/60,
          secs%60,
          usecs/1000,
          usecs%1000
      );
    }
  }
}

int
main(int argc, char *argv[])
{ 
  socks_init();
  imgdb imgdb(argc, argv);

  imgdb.hasFifo = false;

  while (1) {
    // continue to add flow while there are incoming requests
 
    imgdb.fifoTokensCreated = 0;
    imgdb.fifoSegsize = 0;
    imgdb.fifoSendNext = 0;
    imgdb.fifoDatasize = 0;
    imgdb.fifoImgsize = 0;
    imgdb.fifoMss = 0;
    imgdb.totalUsecs = 0;
    imgdb.start.tv_usec = 0;

    while (imgdb.handleqry());

    imgdb.totalUsecs = 0;

    gettimeofday(&imgdb.start, NULL);
    imgdb.fifoDelayUsecs = 0.0;
    imgdb.wfqDelayUsecs = 0.0;

    while (imgdb.started) {
      // fprintf(stderr, "\nSend packet iteration!\n");
      imgdb.sendpkt();
    }
  }
    
#ifdef _WIN32
  WSACleanup();
#endif
  exit(0);
}
