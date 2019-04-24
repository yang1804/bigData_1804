/**
 * Created by 华硕电脑 on 2019/4/23.
 */
(function () {
    var cookieUtils ={
        set:function (cookieName,cookieValue) {
            var cookieText = encodeURIComponent(cookieName)+"="+encodeURIComponent(cookieValue);
            var date = new Date();
            var currentTime = date.getTime();
            var tenYearLongTime=10 * 365 * 24 * 60 * 60 * 1000;
            var tenYearAfter = currentTime+tenYearLongTime;
            date.setTime(tenYearAfter);
            cookieText +=";expires="+date.toUTCString();
            document.cookie=cookieText
        },
        get:function (cookieName) {
            var cookieValue=null;
            var cookieText = document.cookie;
            var items = cookieText.split(";");
            for(index in items){
                var kv=items[index].split("=")
                var key = kv[0].trim();
                var value = kv[1].trim();
                if(key == encodeURIComponent(cookieName)){
                    cookieValue=decodeURIComponent(value)
                }
            }
            return cookieValue;
        }
    };
    var tracker = {
        clientConfig: {
            logServerUrl: "http://hadoopk1/log.gif",
            sessionTimeOut: 2*60*1000,
            logVersion: "2.0"
        },
        cookieKeys: {
            uuid: "uid",
            sid: "sid",
            preVisitTime: "pre_visit_time"
        },
        events: {
            launchEvent: "e_l",//用户首次访问事件
            pageViewEvent: "e_pv",//用户浏览页面事件
            addCartEvent: "e_ad",//商品加入购物车事件
            searchEvent: "e_s"//搜索事件
        },
        columns: {
            eventName: "en",//事件名称
            version: "ver",//日志版本
            platform: "pl",//平台 ios Android
            sdk: "sdk",//sdk js java
            uuid: "uid",//用户唯一标识
            sessionId: "sid",//会话id
            resolution: "b_rst",//浏览器分辨率
            userAgent: "b_usa",//浏览器代理信息
            language: "l",//语言
            clientTime: "ct",//客户端时间
            currentUrl: "url",//当前页面的url
            referrerUrl: "ref",//来源url，上一个页面的url
            title: "tt",//网页标题
            keyword: "kw",//搜索关键字
            goodsId: "gid"//商品id
        },
        setUuid: function (uuid) {
            cookieUtils.set(this.cookieKeys.uuid,uuid)
        },
        getUuid: function () {
            return cookieUtils.get(this.cookieKeys.uuid)
        },
        setSid: function (sid) {
            cookieUtils.set(this.cookieKeys.sid, sid)
        },
        /**
         * 或会话id
         */
        getSid: function () {
            return cookieUtils.get(this.cookieKeys.sid)
        },
        /**
         * 会话开始
         */
        sessionStart: function () {
            if(!this.getSid()){
                this.createNewSession();
            }else{

            }
        },
        createNewSession: function () {
            var sid = guid();
            this.setSid(sid);
            if(!this.getUuid()){
                var uuid=guid();
                this.setUuid(uuid)
            }
        },

        launchEvent: function () {
            var data ={};
            data[this.columns.eventName]=this.events.launchEvent;
            this.setCommonColumns(data)
        },

        setCommonColumns: function (data) {
            //sdk 版本号
            data[this.columns.version] = this.clientConfig.logVersion;
        },

        guid: function () {
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
    }
})