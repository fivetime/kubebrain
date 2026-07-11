package main
import("context";"fmt";"time";clientv3 "go.etcd.io/etcd/client/v3")
func main(){
 cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"172.18.0.2:30079"},DialTimeout:5*time.Second});defer cli.Close()
 idx:=0
 fmt.Println("=== 20x rev=0 per-prefix CountOnly /scale-test/w5/ ===")
 for i:=0;i<20;i++{ctx,cc:=context.WithTimeout(context.Background(),15*time.Second);t:=time.Now()
  r,e:=cli.Get(ctx,"/scale-test/w5/",clientv3.WithPrefix(),clientv3.WithCountOnly());cc()
  if e!=nil{fmt.Printf("  #%d ERR\n",i);continue}
  if time.Since(t)<100*time.Millisecond{idx++}
  _=r;time.Sleep(200*time.Millisecond)}
 fmt.Printf("=> %d/20 served by INDEX\n",idx)
 fmt.Println("=== WHOLE /scale-test CountOnly (10M) — TIMED OUT before the fix ===")
 for i:=0;i<3;i++{ctx,cc:=context.WithTimeout(context.Background(),20*time.Second);t:=time.Now()
  r,e:=cli.Get(ctx,"/scale-test",clientv3.WithPrefix(),clientv3.WithCountOnly());cc()
  if e!=nil{fmt.Printf("  #%d ERR %s\n",i,time.Since(t).Round(time.Millisecond));continue}
  tag:="scan";if time.Since(t)<100*time.Millisecond{tag="INDEX"}
  fmt.Printf("  #%d %-6s %s count=%d\n",i,tag,time.Since(t).Round(time.Millisecond),r.Count)
  time.Sleep(300*time.Millisecond)}
}
