#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include "com_robaho_jnatsd_util_ChannelIO.h"

JNIEXPORT jint JNICALL JavaCritical_com_robaho_jnatsd_util_ChannelIO_write0(jint fd, jlong address, jint len) {
      int n = write(fd,(void*)address,len);
      if(n<0)
         return -1*errno;
      return n;
}

JNIEXPORT jint JNICALL Java_com_robaho_jnatsd_util_ChannelIO_write0(JNIEnv *jenv, jclass clazz, jint fd, jlong address, jint len) {
      return JavaCritical_com_robaho_jnatsd_util_ChannelIO_write0(fd,address,len);
}

JNIEXPORT jint JNICALL JavaCritical_com_robaho_jnatsd_util_ChannelIO_read0(jint fd, jlong address, jint len){
      int n = read(fd,(void*)address,len);
      if(n<0)
         return -1*errno;
      return n;
}

JNIEXPORT jint JNICALL Java_com_robaho_jnatsd_util_ChannelIO_read0(JNIEnv *jenv, jclass clazz, jint fd, jlong address, jint len){
      return JavaCritical_com_robaho_jnatsd_util_ChannelIO_read0(fd,address,len);
}

