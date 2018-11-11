cc -c -I /Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home/include -I /Library/Java/JavaVirtualMachines/jdk-11.jdk/Contents/Home/include/darwin channelio.c
g++ -dynamiclib -o libchannelio.jnilib channelio.o
