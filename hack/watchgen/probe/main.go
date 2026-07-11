package main
import("context";"fmt";"time";clientv3 "go.etcd.io/etcd/client/v3")
func main(){cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"172.18.0.2:30079"},DialTimeout:5*time.Second});defer cli.Close()
 for _,p:=range []string{"/scale-test","/scale-test2","/foload","/foload2","/foload3","/registry","/bootstrap"}{
  t:=time.Now();ctx,cc:=context.WithTimeout(context.Background(),1200*time.Second)
  dr,e:=cli.Delete(ctx,p,clientv3.WithPrefix());cc()
  fmt.Printf("%s deleted=%v in %s err=%v\n",p,func()int64{if dr!=nil{return dr.Deleted};return 0}(),time.Since(t).Round(time.Second),e)}
 fmt.Println("CLEANUP DONE")}
