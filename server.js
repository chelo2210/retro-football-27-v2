const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const rooms = new Map();
const queues = { '1v1': [], '2v2': [] };
const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function code(){
  let s='';
  do { s=''; for(let i=0;i<6;i++) s += alphabet[Math.floor(Math.random()*alphabet.length)]; }
  while(rooms.has(s));
  return s;
}
function send(ws,m){ if(ws && ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(m)); }
function publicPlayers(room){ return room.players.map((p,i)=>({name:p.name||`Jugador ${i+1}`,slot:i})); }
function broadcast(room,m){ room.players.forEach(p=>send(p.ws,m)); }
function needed(room){ return room.mode==='2v2' ? 4 : 2; }

function addToRoom(ws,room,mode){
  if(room.players.length>=4) return false;
  const slot=room.players.length;
  const p={ws,slot,name:'Jugador '+(slot+1),room:room.code};
  room.players.push(p); ws.rf=p;
  if(!room.mode) room.mode=mode==='2v2'?'2v2':'1v1';
  send(ws,{type:'joined',id:String(slot),room:room.code,mode:room.mode,host:slot===0,slot,players:publicPlayers(room)});
  broadcast(room,{type:'roomUpdate',room:room.code,mode:room.mode,players:publicPlayers(room)});
  return true;
}

function startRoom(room){
  if(!room || room.started || room.players.length<needed(room)) return false;
  room.started=true;
  const players=publicPlayers(room);
  room.players.forEach((p,i)=>send(p.ws,{type:'started',room:room.code,mode:room.mode,host:i===0,slot:i,players}));
  return true;
}

function leave(ws){
  const p=ws.rf; if(!p) return;
  const room=rooms.get(p.room);
  if(room){
    room.players=room.players.filter(x=>x!==p);
    if(room.players.length===0){ rooms.delete(room.code); }
    else {
      room.players.forEach((x,i)=>x.slot=i);
      broadcast(room,{type:'roomUpdate',room:room.code,mode:room.mode,players:publicPlayers(room)});
      broadcast(room,{type:'playerLeft',players:publicPlayers(room)});
    }
  }
  ws.rf=null;
}

const server=http.createServer((req,res)=>{
  if(req.url==='/' || req.url==='/health'){
    res.writeHead(200,{'Content-Type':'text/plain; charset=utf-8'});
    res.end('Retro Football 27 Online server OK');
    return;
  }
  res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});
  res.end('Not found');
});

const wss=new WebSocket.Server({server});
wss.on('connection',ws=>{
  ws.on('message',raw=>{
    let m; try{m=JSON.parse(raw)}catch{return;}

    if(m.type==='create'){
      const mode=m.mode==='2v2'?'2v2':'1v1';
      const room={code:code(),mode,players:[],started:false};
      rooms.set(room.code,room);
      addToRoom(ws,room,mode);
      return;
    }

    if(m.type==='join'){
      const room=rooms.get(String(m.room||'').trim().toUpperCase());
      if(!room) return send(ws,{type:'error',message:'Sala no encontrada. Comprueba el código.'});
      if(room.started) return send(ws,{type:'error',message:'La sala ya empezó.'});
      if(room.players.length>=4) return send(ws,{type:'error',message:'La sala está llena.'});
      addToRoom(ws,room,room.mode);
      // For private rooms, start automatically when enough players have joined.
      if(room.players.length>=needed(room)) startRoom(room);
      return;
    }

    if(m.type==='matchmake'){
      const mode=m.mode==='2v2'?'2v2':'1v1';
      queues[mode].push(ws); ws.rfQueue=mode;
      while(queues[mode].length>=needed({mode})){
        const group=queues[mode].splice(0,needed({mode}));
        const room={code:code(),mode,players:[],started:false}; rooms.set(room.code,room);
        group.forEach(x=>addToRoom(x,room,mode));
        startRoom(room);
      }
      return;
    }

    if(m.type==='cancel'){
      const q=ws.rfQueue;
      if(q){queues[q]=queues[q].filter(x=>x!==ws);ws.rfQueue=null;}
      return;
    }

    if(m.type==='start'){
      if(ws.rf?.slot===0) startRoom(rooms.get(ws.rf.room));
      return;
    }

    if((m.type==='input'||m.type==='state') && ws.rf){
      const room=rooms.get(ws.rf.room);
      if(room?.started) room.players.forEach(p=>{if(p.ws!==ws)send(p.ws,{...m,slot:ws.rf.slot});});
    }
  });
  ws.on('close',()=>{
    const q=ws.rfQueue;
    if(q) queues[q]=queues[q].filter(x=>x!==ws);
    leave(ws);
  });
});

server.listen(PORT,()=>console.log(`Retro Football 27 server running on port ${PORT}`));
