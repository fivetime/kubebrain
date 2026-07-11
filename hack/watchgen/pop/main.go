package main
import("context";"fmt";"sync";"sync/atomic";"time";clientv3 "go.etcd.io/etcd/client/v3")
func main(){
 cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"172.18.0.2:30079"},DialTimeout:5*time.Second});defer cli.Close()
 var done int64;var wg sync.WaitGroup;ch:=make(chan int,64);t0:=time.Now()
 for w:=0;w<64;w++{wg.Add(1);go func(){defer wg.Done()
   for i:=range ch{c,cc:=context.WithTimeout(context.Background(),10*time.Second)
     _,e:=cli.Put(c,fmt.Sprintf("/registry/herdload/k%06d",i),"payload-xxxxxxxxxxxxxxxxxxxxxxxxxxxx");cc()
     if e==nil{atomic.AddInt64(&done,1)}}}()}
 for i:=0;i<40000;i++{ch<-i};close(ch);wg.Wait()
 fmt.Printf("populated=%d in %s\n",done,time.Since(t0).Round(time.Second))
}
