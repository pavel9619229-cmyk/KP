<?php
// OPERATOR_REPLY_V10_RENDER_DIRECT_BRIDGE
require($_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php');
$configPath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_config.php';
if(!is_file($configPath)){ http_response_code(503); exit('not configured'); }
$cfg=include $configPath;
$token=(string)($cfg['token']??'');
$secret=(string)($cfg['secret']??'');
$groupId=(string)($cfg['group_chat_id']??'');
$localAgentHash='9da46480c07d05c83f8cf674357110405eee9c156d9f712f710cf0a927568808';
$setupKeyHash='9b7c9e9023484c0aa99882aefa8d5f332a24a79d4f32df1a5a32f9db44b0b161';
function local_agent_ok($expectedHash){
  $got=(string)($_SERVER['HTTP_X_LOCAL_AGENT_TOKEN']??'');
  return $got!=='' && hash_equals($expectedHash,hash('sha256',$got));
}
function max_setup_api($method,$path,$token,$body=null){
  $ch=curl_init('https://platform-api2.max.ru'.$path);
  $headers=['Authorization: '.$token,'Content-Type: application/json'];
  $options=[CURLOPT_CUSTOMREQUEST=>$method,CURLOPT_RETURNTRANSFER=>true,CURLOPT_HTTPHEADER=>$headers,CURLOPT_CONNECTTIMEOUT=>5,CURLOPT_TIMEOUT=>20];
  if(defined('CURLOPT_IPRESOLVE') && defined('CURL_IPRESOLVE_V4')) $options[CURLOPT_IPRESOLVE]=CURL_IPRESOLVE_V4;
  if($body!==null) $options[CURLOPT_POSTFIELDS]=json_encode($body,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES);
  curl_setopt_array($ch,$options);
  $raw=curl_exec($ch); $code=(int)curl_getinfo($ch,CURLINFO_HTTP_CODE); $err=(string)curl_error($ch); curl_close($ch);
  $data=json_decode((string)$raw,true);
  return ['code'=>$code,'data'=>is_array($data)?$data:[],'error'=>$err];
}
function max_subscription_urls($value,&$out){
  if(!is_array($value)) return;
  foreach($value as $key=>$item){
    if($key==='url' && is_string($item)) $out[]=$item;
    elseif(is_array($item)) max_subscription_urls($item,$out);
  }
}
$bridgeMode=(string)($_GET['render_bridge']??'');
if($bridgeMode==='relay'){
  header('Content-Type: application/json; charset=utf-8');
  if(!local_agent_ok($localAgentHash)){ http_response_code(401); echo json_encode(['ok'=>false,'error'=>'unauthorized']); exit; }
  if($_SERVER['REQUEST_METHOD']!=='POST'){ http_response_code(405); echo json_encode(['ok'=>false,'error'=>'method']); exit; }
  $bridgeData=json_decode((string)file_get_contents('php://input'),true);
  if(!is_array($bridgeData)){ http_response_code(400); echo json_encode(['ok'=>false,'error'=>'json']); exit; }
  if(($bridgeData['action']??'')==='ping'){ echo json_encode(['ok'=>true,'mode'=>'relay-ping']); exit; }
  $relayChatId=(string)($bridgeData['chat_id']??'');
  $relayText=trim((string)($bridgeData['text']??''));
  if($relayChatId==='' || $relayText===''){ http_response_code(400); echo json_encode(['ok'=>false,'error'=>'chat_id/text']); exit; }
  if($relayChatId!==$groupId){ http_response_code(403); echo json_encode(['ok'=>false,'error'=>'chat']); exit; }
  if(function_exists('mb_substr')) $relayText=mb_substr($relayText,0,3900,'UTF-8'); else $relayText=substr($relayText,0,3900);
  $ch=curl_init('https://platform-api2.max.ru/messages?chat_id='.rawurlencode($relayChatId));
  $options=[CURLOPT_POST=>true,CURLOPT_RETURNTRANSFER=>true,CURLOPT_HTTPHEADER=>['Authorization: '.$token,'Content-Type: application/json'],CURLOPT_POSTFIELDS=>json_encode(['text'=>$relayText],JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES),CURLOPT_CONNECTTIMEOUT=>5,CURLOPT_TIMEOUT=>20];
  if(defined('CURLOPT_IPRESOLVE') && defined('CURL_IPRESOLVE_V4')) $options[CURLOPT_IPRESOLVE]=CURL_IPRESOLVE_V4;
  curl_setopt_array($ch,$options);
  $relayRaw=curl_exec($ch); $relayCode=(int)curl_getinfo($ch,CURLINFO_HTTP_CODE); $relayErrno=(int)curl_errno($ch); $relayError=(string)curl_error($ch); curl_close($ch);
  @file_put_contents($_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_events.log',date('c').' RENDER_DIRECT_RELAY max_code='.$relayCode.' errno='.$relayErrno.' error='.preg_replace('/\s+/u',' ',$relayError).PHP_EOL,FILE_APPEND|LOCK_EX);
  $relayOk=$relayCode===200; http_response_code($relayOk?200:502); echo json_encode(['ok'=>$relayOk,'max_code'=>$relayCode,'errno'=>$relayErrno,'error'=>$relayError],JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); exit;
}
if($bridgeMode==='setup'){
  header('Content-Type: application/json; charset=utf-8');
  $setupKey=(string)($_GET['key']??'');
  if($setupKey==='' || !hash_equals($setupKeyHash,hash('sha256',$setupKey))){ http_response_code(403); echo json_encode(['ok'=>false,'error'=>'forbidden']); exit; }
  $donePath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_render_setup_done.json';
  if(is_file($donePath)){ echo json_encode(['ok'=>true,'already_done'=>true]); exit; }
  $oldUrl='https://shina-moskva.ru/max-webhook.php';
  $newUrl='https://onec-kp-realtime.onrender.com/api/max/webhook';
  $before=max_setup_api('GET','/subscriptions',$token);
  $delete=max_setup_api('DELETE','/subscriptions?url='.rawurlencode($oldUrl),$token);
  $create=max_setup_api('POST','/subscriptions',$token,['url'=>$newUrl,'update_types'=>['message_created'],'secret'=>$secret]);
  $after=max_setup_api('GET','/subscriptions',$token);
  $urls=[]; max_subscription_urls($after['data'],$urls); $urls=array_values(array_unique($urls));
  $createOk=$create['code']===200 && !empty($create['data']['success']);
  $deleteOk=in_array($delete['code'],[200,404],true) && (!isset($delete['data']['success']) || !empty($delete['data']['success']) || $delete['code']===404);
  $result=['ok'=>$createOk,'old_delete_ok'=>$deleteOk,'delete_code'=>$delete['code'],'create_code'=>$create['code'],'active_urls'=>$urls,'create_error'=>(string)($create['data']['message']??$create['error']??'')];
  if($createOk) @file_put_contents($donePath,json_encode(['completed_at'=>date('c'),'new_url'=>$newUrl],JSON_UNESCAPED_SLASHES),LOCK_EX);
  @file_put_contents($_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_events.log',date('c').' RENDER_DIRECT_SETUP delete_code='.$delete['code'].' create_code='.$create['code'].' ok='.($createOk?'1':'0').PHP_EOL,FILE_APPEND|LOCK_EX);
  http_response_code($createOk?200:502); echo json_encode($result,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES); exit;
}
if($bridgeMode==='forward'){
  if(!local_agent_ok($localAgentHash)){ http_response_code(401); header('Content-Type: application/json; charset=utf-8'); echo json_encode(['ok'=>false,'error'=>'unauthorized']); exit; }
  if($_SERVER['REQUEST_METHOD']!=='POST'){ http_response_code(405); exit('method'); }
  $forwardRaw=(string)file_get_contents('php://input');
  $forwardProbe=json_decode($forwardRaw,true);
  if(is_array($forwardProbe) && ($forwardProbe['action']??'')==='ping'){ header('Content-Type: application/json; charset=utf-8'); echo json_encode(['ok'=>true,'mode'=>'forward-ping']); exit; }
  $got=$secret;
} else {
  $got=(string)($_SERVER['HTTP_X_MAX_BOT_API_SECRET']??'');
}
if($secret==='' || !hash_equals($secret,$got)){ http_response_code(403); exit('forbidden'); }
if($_SERVER['REQUEST_METHOD']!=='POST'){ http_response_code(405); exit('method'); }
$raw=isset($forwardRaw)?$forwardRaw:file_get_contents('php://input');
$update=json_decode($raw,true);
if(!is_array($update)){ http_response_code(400); exit('json'); }
http_response_code(200); header('Content-Type: text/plain; charset=utf-8'); echo 'OK';
if(function_exists('fastcgi_finish_request')) fastcgi_finish_request();
ignore_user_abort(true);
function max_post($query,$token,$body){
  $ch=curl_init('https://platform-api2.max.ru/messages?'.$query);
  $json=json_encode($body,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES);
  curl_setopt_array($ch,[CURLOPT_POST=>true,CURLOPT_RETURNTRANSFER=>true,CURLOPT_HTTPHEADER=>['Authorization: '.$token,'Content-Type: application/json'],CURLOPT_POSTFIELDS=>$json,CURLOPT_CONNECTTIMEOUT=>3,CURLOPT_TIMEOUT=>8]);
  $raw=curl_exec($ch); $code=(int)curl_getinfo($ch,CURLINFO_HTTP_CODE); curl_close($ch);  $data=json_decode((string)$raw,true); return ['code'=>$code,'data'=>is_array($data)?$data:[]];
}
function max_text($query,$token,$text){ return max_post($query,$token,['text'=>$text]); }
function is_kp_588_command($text){ return preg_match('/^\s*(?:\x{041A}\x{041F}|KP)\s*(?:\x{2116})?\s*0*588\s*$/ui',(string)$text)===1; }
function render_kp_588(){
  $host='onec-kp-realtime.onrender.com';
  $url='https://'.$host.'/api/max/test/kp-588';
  $attempts=[
    ['label'=>'dns','resolve'=>[]],
    ['label'=>'cf-216.24.57.7','resolve'=>[$host.':443:216.24.57.7']],
    ['label'=>'cf-216.24.57.15','resolve'=>[$host.':443:216.24.57.15']],
  ];
  $errors=[];
  foreach($attempts as $attempt){
    $ch=curl_init($url);
    $headers=['Accept: application/json','User-Agent: Shina-Moskva-MAX-Bridge/1.1','Connection: close'];
    $options=[CURLOPT_RETURNTRANSFER=>true,CURLOPT_HTTPHEADER=>$headers,CURLOPT_CONNECTTIMEOUT=>15,CURLOPT_TIMEOUT=>60];
    if(defined('CURLOPT_IPRESOLVE') && defined('CURL_IPRESOLVE_V4')) $options[CURLOPT_IPRESOLVE]=CURL_IPRESOLVE_V4;
    if(defined('CURLOPT_SSLVERSION') && defined('CURL_SSLVERSION_TLSv1_2')) $options[CURLOPT_SSLVERSION]=CURL_SSLVERSION_TLSv1_2;
    if(defined('CURLOPT_HTTP_VERSION') && defined('CURL_HTTP_VERSION_1_1')) $options[CURLOPT_HTTP_VERSION]=CURL_HTTP_VERSION_1_1;
    if(!empty($attempt['resolve']) && defined('CURLOPT_RESOLVE')) $options[CURLOPT_RESOLVE]=$attempt['resolve'];
    curl_setopt_array($ch,$options);
    $started=microtime(true);
    $raw=curl_exec($ch);
    $code=(int)curl_getinfo($ch,CURLINFO_HTTP_CODE);
    $errno=(int)curl_errno($ch);
    $err=(string)curl_error($ch);
    $elapsed=round(microtime(true)-$started,2);
    curl_close($ch);
    $data=json_decode((string)$raw,true);
    if($code===200 && is_array($data) && !empty($data['text'])) return ['ok'=>true,'text'=>(string)$data['text'],'code'=>$code,'error'=>'','route'=>$attempt['label']];
    $errors[]=$attempt['label'].' code='.$code.' errno='.$errno.' time='.$elapsed.' error='.($err!==''?$err:'invalid response');
  }
  return ['ok'=>false,'text'=>'','code'=>0,'error'=>implode(' | ',$errors),'route'=>''];
}
function send_kp_588($query,$token){
  $kp=render_kp_588();
  $text=$kp['ok']?$kp['text']:('Render: KP 588 is unavailable. HTTP '.(string)$kp['code'].'.');
  $sent=max_text($query,$token,$text);
  bridge_log('KP588 route='.(string)($kp['route']??'').' render_code='.$kp['code'].' max_code='.$sent['code'].' error='.preg_replace('/\s+/u',' ',(string)$kp['error']));
  return $sent;
}
function state_set($path,$operatorId,$target){
  $fh=@fopen($path,'c+'); if(!$fh) return false;
  if(!flock($fh,LOCK_EX)){ fclose($fh); return false; }
  $old=stream_get_contents($fh); $state=json_decode((string)$old,true); if(!is_array($state)) $state=[];
  $state[(string)$operatorId]=['uid'=>(string)($target['uid']??''),'chat_id'=>(string)($target['chat_id']??''),'time'=>time()];
  ftruncate($fh,0); rewind($fh); fwrite($fh,json_encode($state,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES)); fflush($fh);
  flock($fh,LOCK_UN); fclose($fh); return true;
}
function state_take($path,$operatorId){
  $fh=@fopen($path,'c+'); if(!$fh) return ['uid'=>'','chat_id'=>''];
  if(!flock($fh,LOCK_EX)){ fclose($fh); return ['uid'=>'','chat_id'=>'']; }
  $old=stream_get_contents($fh); $state=json_decode((string)$old,true); if(!is_array($state)) $state=[];
  $key=(string)$operatorId; $target=['uid'=>'','chat_id'=>''];
  if(isset($state[$key]) && is_array($state[$key])){
    $age=time()-(int)($state[$key]['time']??0);
    if($age>=0 && $age<=1800) $target=['uid'=>(string)($state[$key]['uid']??''),'chat_id'=>(string)($state[$key]['chat_id']??'')];
    unset($state[$key]);
  }
  ftruncate($fh,0); rewind($fh); fwrite($fh,json_encode($state,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES)); fflush($fh);
  flock($fh,LOCK_UN); fclose($fh); return $target;
}function command_label($path,$name,$uid,$chatId){
  $fh=@fopen($path,'c+'); if(!$fh) return 'Ответить '.$name;
  if(!flock($fh,LOCK_EX)){ fclose($fh); return 'Ответить '.$name; }
  $old=stream_get_contents($fh); $map=json_decode((string)$old,true); if(!is_array($map)) $map=[];
  $base='Ответить '.trim($name); $label=$base; $n=2;
  while(isset($map[$label]) && (string)($map[$label]['uid']??'')!==(string)$uid){ $label=$base.' ('.$n.')'; $n++; }
  $map[$label]=['uid'=>(string)$uid,'chat_id'=>(string)$chatId,'time'=>time()];
  if(count($map)>1000){ uasort($map,function($a,$b){ return (int)($a['time']??0)<=>(int)($b['time']??0); }); $map=array_slice($map,-1000,null,true); }
  ftruncate($fh,0); rewind($fh); fwrite($fh,json_encode($map,JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES)); fflush($fh);
  flock($fh,LOCK_UN); fclose($fh); return $label;
}
function command_target($path,$label){
  if($label==='') return ['uid'=>'','chat_id'=>''];
  $raw=@file_get_contents($path); $map=json_decode((string)$raw,true); if(!is_array($map)) return ['uid'=>'','chat_id'=>''];
  if(!isset($map[$label]) || !is_array($map[$label])) return ['uid'=>'','chat_id'=>''];
  return ['uid'=>(string)($map[$label]['uid']??''),'chat_id'=>(string)($map[$label]['chat_id']??'')];
}
function bridge_log($msg){ @file_put_contents($_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_events.log',date('c').' '.$msg.PHP_EOL,FILE_APPEND|LOCK_EX); }
function seen_before($path,$key){
  $fh=@fopen($path,'c+'); if(!$fh) return false;
  if(!flock($fh,LOCK_EX)){ fclose($fh); return false; }
  $old=stream_get_contents($fh); $rows=array_values(array_filter(preg_split('/\R/',(string)$old)));
  $dup=in_array($key,$rows,true);
  if(!$dup){ $rows[]=$key; if(count($rows)>300) $rows=array_slice($rows,-300); ftruncate($fh,0); rewind($fh); fwrite($fh,implode(PHP_EOL,$rows).PHP_EOL); fflush($fh); }
  flock($fh,LOCK_UN); fclose($fh); return $dup;
}$seenPath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_seen.log';
$statePath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_operator_state.json';
$commandPath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_command_map.json';
$key=hash('sha256',$raw); if(seen_before($seenPath,$key)) return;
if(($update['update_type']??'')!=='message_created') return;
$m=$update['message']??[]; $sender=$m['sender']??[]; $recipient=$m['recipient']??[];
if(!empty($sender['is_bot'])) return;
$text=trim((string)($m['body']['text']??''));
$chatType=(string)($recipient['chat_type']??'');
$chatId=(string)($recipient['chat_id']??'');
if($chatType==='chat' && $chatId===$groupId && $text!==''){
  if(is_kp_588_command($text)){ send_kp_588('chat_id='.rawurlencode($groupId),$token); return; }
  $operatorId=(string)($sender['user_id']??'');
  $mapped=command_target($commandPath,$text);
  if($operatorId!=='' && ($mapped['chat_id']!=='' || $mapped['uid']!=='')){
    state_set($statePath,$operatorId,$mapped);
    bridge_log('SELECT operator='.$operatorId.' client='.$mapped['uid'].' dialog='.$mapped['chat_id']);
    $shown=preg_replace('/^Ответить\s+/u','',$text);
    max_text('chat_id='.rawurlencode($groupId),$token,'Клиент выбран: '.$shown.'. Напиши ответ следующим сообщением.');
    return;
  }
  $target=['uid'=>'','chat_id'=>'']; $out=$text;
  if(preg_match('/^\/reply\s+(\d+)\s+(.+)$/us',$text,$mm)){ $target=['uid'=>$mm[1],'chat_id'=>'']; $out=$mm[2]; }
  else { $target=state_take($statePath,$operatorId); }
  if($target['chat_id']!=='' || $target['uid']!==''){    if($target['chat_id']!==''){ $query='chat_id='.rawurlencode($target['chat_id']); $route='chat_id'; }
    else { $query='user_id='.rawurlencode($target['uid']); $route='user_id'; }
    $r=max_text($query,$token,$out);
    $err=''; if(isset($r['data']['message'])) $err=' err='.preg_replace('/\s+/u',' ',(string)$r['data']['message']);
    bridge_log('OUT operator='.$operatorId.' client='.$target['uid'].' dialog='.$target['chat_id'].' route='.$route.' code='.$r['code'].$err);
  }
  return;
}
if($chatType!=='dialog') return;
$uid=(string)($sender['user_id']??'');
if($uid==='' || $text==='') return;
if(is_kp_588_command($text)){ send_kp_588('chat_id='.rawurlencode($chatId),$token); return; }
$name=trim((string)($sender['first_name']??'').' '.(string)($sender['last_name']??''));
if($name==='') $name='Client';
$forward="MAX inquiry\nFrom: {$name}\nuser_id: {$uid}\n\n{$text}";
$buttonText=command_label($commandPath,$name,$uid,$chatId);
$keyboard=['type'=>'inline_keyboard','payload'=>['buttons'=>[[['type'=>'message','text'=>$buttonText]]]]];
$r=max_post('chat_id='.rawurlencode($groupId),$token,['text'=>$forward,'attachments'=>[$keyboard]]);
bridge_log('IN client='.$uid.' dialog='.$chatId.' group_code='.$r['code']);