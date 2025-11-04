
from utils import start_actor, call_remote

from dtw.fed_object import DtwObject

pp1_id={
    'cmail':"0.0.0.0",
    'cport':"50051",
    'dmail':"0.0.0.0",
    'dport':"50052",
}
pp1_cmail = pp1_id["cmail"]+":"+pp1_id["cport"]

pp2_id={
    'cmail':"0.0.0.0",
    'cport':"50053",
    'dmail':"0.0.0.0",
    'dport':"50054",
}
pp2_cmail = pp2_id["cmail"]+":"+pp2_id["cport"]

start_actor(pp1_cmail,pp1_id,101)
start_actor(pp2_cmail,pp2_id,12)

for i in range(10,13):
    # 调用 add
    res = call_remote(pp1_cmail, "double", i)

    ip,port=res.SrcIpPort.split(':')
    dobj=DtwObject(res.Objid,ip,eval(port))

    res = call_remote(pp2_cmail, "double", dobj)
    print(res)