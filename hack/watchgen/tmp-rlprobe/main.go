package main
import("context";"fmt";"sort";"time";clientv3 "go.etcd.io/etcd/client/v3")
func main(){
 c,_:=clientv3.New(clientv3.Config{Endpoints:[]string{"10.224.0.14:3379"},DialTimeout:5*time.Second});defer c.Close()
 var lat []int64
 for i:=0;i<30;i++{ctx,cc:=context.WithTimeout(context.Background(),5*time.Second)
  t:=time.Now();c.Get(ctx,"/registry/readlat-probe");lat=append(lat,time.Since(t).Microseconds());cc()}
 sort.Slice(lat,func(i,j int)bool{return lat[i]<lat[j]})
 fmt.Printf("%.1f\n",float64(lat[15])/1000)
}
