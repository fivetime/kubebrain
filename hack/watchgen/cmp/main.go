package main
import("context";"fmt";"os";"strconv";"time";clientv3 "go.etcd.io/etcd/client/v3")
func q(cli *clientv3.Client,name string,rev int64){ctx,cc:=context.WithTimeout(context.Background(),12*time.Second);t:=time.Now()
 opts:=[]clientv3.OpOption{clientv3.WithPrefix(),clientv3.WithCountOnly()};if rev>0{opts=append(opts,clientv3.WithRev(rev))}
 r,e:=cli.Get(ctx,"/scale-test/w5/",opts...);cc()
 if e!=nil{fmt.Printf("  %-14s ERR %s\n",name,time.Since(t).Round(time.Millisecond));return}
 tag:="scan";if time.Since(t)<100*time.Millisecond{tag="INDEX"}
 fmt.Printf("  %-14s %-6s %s count=%d\n",name,tag,time.Since(t).Round(time.Millisecond),r.Count)}
func main(){
 cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"172.18.0.2:30079"},DialTimeout:5*time.Second});defer cli.Close()
 R0,_:=strconv.ParseInt(os.Args[1],10,64)
 g,_:=context.WithTimeout(context.Background(),8*time.Second)
 gr,_:=cli.Get(g,"/scale-test/w5/k00000000",clientv3.WithLimit(1))
 cur:=gr.Header.Revision
 fmt.Printf("rebuildRev=%d current=%d gap=%d\n",R0,cur,cur-R0)
 q(cli,"@rebuildRev",R0); q(cli,"@current(0)",0)
 // sweep to find readyRev boundary
 for _,d:=range []int64{1,2,5,20,100}{ q(cli,fmt.Sprintf("@R0+%d",d),R0+d) }
}
