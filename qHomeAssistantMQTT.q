\l mqtt.q
\l qjinja.q
port:1883
broker_address:.z.x[0]
HDB:hsym `$.z.x[1]
discoveryPrefix:"homeassistant"
hour:{`int$sum 24 1*`date`hh$\:x}
intToTS:{`timestamp$`long$0D01*x} 
cHour:hour .z.p
.z.zd:17 2 6

sensorConfigHist:([] name:`$();topic:`$();state_topic:`$();opts:())
sensorStateHist:([] int:`int$();time:`timestamp$();name:`$();state:())
sensorConfig:([name:`$()] topic:`$();state_topic:`$();opts:())
sensorState:([] time:`timestamp$();name:`$();state:())

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
 sensors:select name,value_template:opts[;`value_template] from sensorConfig where state_topic=`$top;
 sensors:update time:now,val:{{$[x~"None";0Nf;"F"$x]}.qjinja.extract[x;y]}[;msg] each value_template from sensors;
 `sensorState insert value exec time,name,val from sensors where not null val
 }

.mqtt.msgrcvd:{[topic;msg]
  now:.z.p;
  if[cHour<hour now;
      writeToDisk[now];
     ];
  if[topic like discoveryPrefix,"/sensor/*/config";
    opts:.j.k msg;
    .mqtt.sub[`$opts`state_topic];
    `sensorConfig upsert (`$opts`name;`$topic;`$opts`state_topic;opts);:(::)];
  if[(`$topic) in exec state_topic from sensorConfig;
     store[now;topic;msg]];
 }

queryState:{[sensor;sTime;eTime]
  hist:delete int from select from sensorStateHist where int within hour (sTime;eTime),name like sensor,time within (sTime;eTime);
  realtime:select from sensorState where name like sensor,time within (sTime;eTime);
  hist,realtime
 }
