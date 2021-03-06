.qjinja.init:{[]
  .qjinja.filePath:{x -3+count x} value .z.s;
  slash:$[.z.o like "w*";"\\";"/"];
  .qjinja.basePath:slash sv -1_slash vs .qjinja.filePath;
  if[not `p in key `;system"l ",getenv[`QHOME],slash,"p.q"];
  .p.e"import sys";
  .p.e "sys.path.append(\"",ssr[;"\\";"\\\\"] .qjinja.basePath,"\")";
  .qjinja.py.lib:.p.import`qjinja;
  };

.qjinja.init[];

.qjinja.extract:{[template;msg]
  .qjinja.py.lib[`:extract][template; msg]`
  };
