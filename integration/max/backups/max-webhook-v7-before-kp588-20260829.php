<?php
// OPERATOR_REPLY_V7_CHAT_ID
require($_SERVER['DOCUMENT_ROOT'].'/bitrix/modules/main/include/prolog_before.php');
$configPath=$_SERVER['DOCUMENT_ROOT'].'/bitrix/php_interface/max_bridge_config.php';
if(!is_file($configPath)){ http_response_code(503); exit('not configured'); }
$cfg=include $configPath;
$token=(string)($cfg['token']??'');
$secret=(string)($cfg['secret']??'');
$groupId=(string)($cfg['group_chat_id']??'');
$got=(string)($_SERVER['HTTP_X_MAX_BOT_API_SECRET']??'');
if($secret==='' || !hash_equals($secret,$got)){ http_response_code(403); exit('forbidden'); }
if($_SERVER['REQUEST_METHOD']!=='POST'){ http_response_code(405); exit('method'); }
$raw=file_get_contents('php://input');
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
$name=trim((string)($sender['first_name']??'').' '.(string)($sender['last_name']??''));
if($name==='') $name='Client';
$forward="MAX inquiry\nFrom: {$name}\nuser_id: {$uid}\n\n{$text}";
$buttonText=command_label($commandPath,$name,$uid,$chatId);
$keyboard=['type'=>'inline_keyboard','payload'=>['buttons'=>[[['type'=>'message','text'=>$buttonText]]]]];
$r=max_post('chat_id='.rawurlencode($groupId),$token,['text'=>$forward,'attachments'=>[$keyboard]]);
bridge_log('IN client='.$uid.' dialog='.$chatId.' group_code='.$r['code']);