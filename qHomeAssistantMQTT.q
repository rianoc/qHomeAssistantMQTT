\l mqtt.q
port:1883
broker_address:.z.x[0]
HDB:hsym `$.z.x[1]
discoveryPrefix:"homeassistant"
hour:{`int$sum 24 1*`date`hh$x}
intToTS:{`timestamp$`long$0D01*x} 
cHour:hour .z.p
.z.zd:17 2 6

sensorConfigHist:([] name:`$();topic:`$();state_topic:`$();payload:())
sensorStateHist:([] int:`int$();time:`timestamp$();node:`$();name:`$();state:())
sensorConfig:([name:`$()] topic:`$();state_topic:`$();payload:())
sensorState:([] time:`timestamp$();node:`$();name:`$();state:())

if[count key HDB;
   system"l ",1_string HDB;
   sensorConfigHist:1!select from sensorConfigHist];

.mqtt.conn[`$"localhost:",string port;`src;()!()]

.mqtt.sub[`$discoveryPrefix,"/#"]

writeToDisk:{[now]
  .Q.dd[HDB;(`$string cHour;`sensorStateHist;`)] upsert .Q.ens[HDB;sensorState;`sensors];
  `sensorState set 0#sensorState;
  `cHour set hour now;
  .Q.dd[HDB;(`sensorConfigHist;`)] set .Q.ens[HDB;0!sensorConfig;`sensors];
  system"l ",1_string HDB;
 }

.z.exit:{
  @[writeToDisk;.z.p;{show "Failed to store data on exit"}]
 }

store:{[now;top;msg]
 msg:.j.k msg;
 n:count msg;
 d:`object_id`node_id!@[;0 1]reverse "/" vs neg[count "/state"]_count[discoveryPrefix,"/sensor/"]_.debug.thing[0];
 `sensorState insert (n#now;`$n#enlist d`node_id;`$d[`object_id],/:string key msg;value msg)
 }

.mqtt.msgrcvd:{[top;msg]
  .debug.thing:(top;msg);
  now:.z.p;
  area:`$first -2#"/" vs top;
  if[cHour<hour now;
      writeToDisk[now];
     ];
  if[top like discoveryPrefix,"/sensor/*config";
    payload:.j.k msg;
    `sensorConfig upsert (`$payload`name;`$top;`$payload`state_topic;payload);:(::)];
  if[top like discoveryPrefix,"/sensor/*state";
     store[now;top;msg]];
 }

queryState:{[sensor;sTime;eTime]
  select from sensorState where name=sensor,time within (sTime;eTime)
 }
