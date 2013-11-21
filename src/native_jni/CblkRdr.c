#include"CblkRdr.h"
#include<sys/stat.h>
#include<fcntl.h>

JNIEXPORT jstring JNICALL Java_CblkRdr_blkrdr(JNIEnv *env, jobject obj, jint blkSize, jint blkNum, jint tupleLen, jstring filename){


	char line[1000],file[30];
	jclass cls;
	jstring value;
	jsize length;
	int fildes;
	length = (*env)->GetStringUTFLength(env,filename);
	(*env)->GetStringUTFRegion(env,filename, (jsize)0,length,file);
	fildes=open(file,O_RDONLY);


/*
printf("fildes::%d",fildes);
*/


	cblkrdr(line,(int)blkSize,(int)blkNum,(int)tupleLen,fildes);
	value=(jstring)(*env)->NewStringUTF(env,line);



/*
 * printf("retrived::: %s",line);
 *
 */

return value;

}


JNIEXPORT jint JNICALL Java_CblkRdr_totallines(JNIEnv *env, jobject obj, jint tupleLen, jstring filename){
	
	int lines,fildes;
	char file[30];
	jsize length;
	length = (*env)->GetStringUTFLength(env,filename);
	(*env)->GetStringUTFRegion(env,filename, (jsize)0,length,file);
	fildes = open(file,O_RDONLY);
	//printf("entered c program %s::%d\n",file,fildes);
	lines = totallines(fildes,(int)tupleLen);
	//printf("entered c program\n");	
	return (jint)lines;

}

