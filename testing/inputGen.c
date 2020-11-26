#include <stdio.h>
#include <stdlib.h>
int main(int argc,char** argv){
    int cnt = 10;
    freopen("sample2.txt","w",stdout);
    if(argc > 1)
        cnt = atoi(argv[1]);
    cnt -= 1;
    printf("%d",cnt);
    while(cnt-- && printf("\n"))printf("%d",cnt);    
}