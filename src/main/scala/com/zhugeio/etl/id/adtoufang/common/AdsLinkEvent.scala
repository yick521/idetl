package com.zhugeio.etl.id.adtoufang.common

class AdsLinkEvent {

  var link_id:Integer=0
  var event_id:Integer=0
  var channel_event=""
  var match_json=""
  var frequency:Integer=0
  var event_ids:String=""
  var window_time:Long=0L
  def toJsonString(): String = {
    "{\"link_id\":\"" + link_id +
      "\",\"event_id\":" + event_id +
      ",\"channel_event\":\"" + channel_event +
      "\",\"match_json\":" + match_json +
      ",\"frequency\":\"" + frequency +
      "\"}"
  }
}
