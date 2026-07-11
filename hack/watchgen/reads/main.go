package main
import("context";"fmt";"time";clientv3 "go.etcd.io/etcd/client/v3")
func main(){
 cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"172.18.0.2:30079"},DialTimeout:5*time.Second});defer cli.Close()
 tm:=func(name string,f func(context.Context)(string,error),to time.Duration){best:=time.Hour;var info string;var e error
  for i:=0;i<4;i++{ctx,cc:=context.WithTimeout(context.Background(),to);t:=time.Now();info,e=f(ctx);d:=time.Since(t);cc()
   if e!=nil{fmt.Printf("  %-34s ERR %s\n",name,d.Round(time.Millisecond));return};if d<best{best=d}}
  fmt.Printf("  %-34s %s  %s\n",name,best.Round(time.Millisecond),info)}
 tm("point Get",func(ctx context.Context)(string,error){r,e:=cli.Get(ctx,"/scale-test2/w5/k00001000");if e!=nil{return"",e};return fmt.Sprintf("kvs=%d",len(r.Kvs)),nil},8*time.Second)
 tm("per-prefix List page (limit 500)",func(ctx context.Context)(string,error){r,e:=cli.Get(ctx,"/scale-test2/w5/",clientv3.WithPrefix(),clientv3.WithLimit(500));if e!=nil{return"",e};return fmt.Sprintf("kvs=%d more=%v",len(r.Kvs),r.More),nil},15*time.Second)
 tm("per-prefix CountOnly (~26k)",func(ctx context.Context)(string,error){r,e:=cli.Get(ctx,"/scale-test2/w5/",clientv3.WithPrefix(),clientv3.WithCountOnly());if e!=nil{return"",e};return fmt.Sprintf("count=%d",r.Count),nil},20*time.Second)
}
