#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include "com_robaho_jnatsd_util_ChannelIO.h"

JNIEXPORT jint JNICALL Java_com_robaho_jnatsd_util_ChannelIO_write0(JNIEnv *jenv, jclass clazz, jint fd, jlong address, jint len) {
      int n = write(fd,(void*)address,len);
      if(n<0)
         return -1*errno;
      return n;   
}

/*
 * Class:     com_robaho_jnatsd_util_ChannelIO
 * Method:    read0
 * Signature: (IJI)I
 */
JNIEXPORT jint JNICALL Java_com_robaho_jnatsd_util_ChannelIO_read0(JNIEnv *jenv, jclass clazz, jint fd, jlong address, jint len){
    int n = read(fd,(void*)address,len);
      if(n<0)
         return -1*errno;
      return n;   
}
