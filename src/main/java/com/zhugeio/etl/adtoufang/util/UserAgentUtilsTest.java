package com.zhugeio.etl.adtoufang.util;

import com.zhugeio.etl.id.adtoufang.util.ToolUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentUtilsTest {
    private static List<String> uaCase = new ArrayList<>();

    private static void uadd(String s) {
        uaCase.add(s);
    }

    static void init() {

//        uadd("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)");
//        uadd("dadaShop/8.22.0 (com.dada.store; build:615; iOS 18.8.1) Alamofire/8.22.0");
    }


    @Test
    public void harmonyTest() {
        init();
        String pattern = "linux;.*android.*(harmony).*;\\s*([\\w.,/\\-]+)\\s*[;)]";
        int i = 0;
        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        for (String s : uaCase) {
            Matcher m = r.matcher(s);
            if (m.find()) {
                System.out.println(i + " : " + m.group(0));
                System.out.println("name : " + m.group(1));
                System.out.println("version : " + m.group(2) + "\n");
                i++;
            }
        }
    }

    @Test
    public void androidTest() {
        //andriod
        uadd("Mozilla/5.0 (Linux; Android 8.1.0; DUB-AL00 Build/HUAWEIDUB-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; Android 9.1.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; Android 10.1.0 ;) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; U; Android 10; zh-CN; HLK-AL00 Build/HONORHLK-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/69.0.3497.100 UWS/3.22.2.28 Mobile Safari/537.36 UCBS/3.22.2.28_210922181100 ChannelId(0) NebulaSDK/1.8.100112 Nebula AlipayDefined(nt:WIFI,ws:360|0|3.0) AliApp(AP/10.2.36.8000) AlipayClient/10.2.36.8000 Language/zh-Hans useStatusBar/true isConcaveScreen/false Region/CNAriver/1.0.0");
        uadd("Mozilla/5.0 (Linux; Android 11; V2055A; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/87.0.4280.141 Mobile Safari/537.36 VivoBrowser/10.3.18.0");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-cn; PEGM00 Build/RKQ1.200903.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/70.0.3538.80 Mobile Safari/537.36 HeyTapBrowser/40.7.31.1");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-CN; Mi 10 Build/RKQ1.200826.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 UCBrowser/13.6.7.1148 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-cn; Redmi K30 Build/RKQ1.200826.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.116 Mobile Safari/537.36 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (Linux; Android 10; HarmonyOS; ELS-AN00; HMSCore 6.2.0.302) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 10; HarmonyOS; ELS-AN01) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 11; NTH-AN00; HMSCore 6.2.0.302) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 10; SEA-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; U; Android 9; zh-cn; KB2000 Build/RP1A.201005.001) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/12.1 Mobile Safari/537.36 COVC/045825");
        uadd("Mozilla/5.0 (Linux; Android 9; V1901A Build/P00610; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.27 SP-engine/2.37.0 baiduboxapp/12.28.5.10 (Baidu; P1 9) NABar/1.0");

        uadd("Mozilla/5.0+(Linux;+Android+12;+LGE-AN00+Build/BRAND-MODEL;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/76.0.3809.89+Mobile+Safari/537.36+T7/12.9+SP-engine/2.21.0+matrixstyle/0+lite+baiduboxapp/5.4.0.10+(Baidu;+P1+8.1.0)+NABar/1.0");
        uadd("Mozilla/5.0+(Linux;+Android+9;+RVL-AL09+Build/BRAND-MODEL;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/76.0.3809.89+Mobile+Safari/537.36+T7/12.9+SP-engine/2.21.0+matrixstyle/0+lite+baiduboxapp/5.4.0.10+(Baidu;+P1+8.1.0)+NABar/1.0");
        uadd("Dalvik/2.1.0 (Linux; U; Android 9; RVL-AL09 Build/HUAWEIRVL-AL09)");
        uadd("Dalvik/2.1.0 (Linux; U; Android 12; LGE-AN00 Build/HONORLGE-AN00)");
//        uadd("");
//        uadd("");

//        uadd("Mozilla/5.0 (Linux; Android; SEA-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.64 HuaweiBrowser/10.0.3.311 Mobile Safari/537.36");
//        uadd("Mozilla/5.0 (Linux; U; Android ; zh-cn; LON-AL00 Build/HUAWEILON-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/12.1 Mobile Safari/537.36 COVC/045825");
//        uadd("Mozilla/5.0 (Linux; U; Android ) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/12.1 Mobile Safari/537.36 COVC/045825");

        //"linux;.*(android).([\\w.,/\\-]+).*[;)]"
        String ppp = "[^a-zA-Z0-9 ]";
        Pattern pattern = Pattern.compile("linux;.*(android).([\\w.,/\\-]+)", Pattern.CASE_INSENSITIVE);
        int i = 0;
        for (String s : uaCase) {
            System.out.println(s);
            Matcher matcher = pattern.matcher(s);
            if (matcher.find()) {
                System.out.println(i + " : " + matcher.group(0));
                System.out.println("name : " + matcher.group(1));
                System.out.println("version : " + matcher.group(2).replaceAll(ppp, ".") + "\n");
                i++;
            }
        }
    }

    @Test
    public void deviceTest() {
        init();
        String ppp = "[^a-zA-Z0-9 ]";
        List<String> pList = Arrays.asList(
                ";\\s*([\\w.,/\\- ]+)\\sbuild/",
                "linux;.*android.*harmony.*;\\s*([\\w.,/\\- ]+);.*HuaWeiBrowser",
                "linux;.*android.*harmony.*;\\s*([\\w.,/\\- ]+)[)].*HuaWeiBrowser",
                "linux;.*android.*;\\s*([\\w.,/\\- ]+);.*HuaWeiBrowser",
                "linux;.*android.*;\\s*([\\w.,/\\- ]+)[)].*HuaWeiBrowser",
                ".*android.*;\\s*([\\w.,/\\- ]+); wv[)].*VivoBrowser"
        );
        for (String rp : pList) {
            int i = 0;
            Pattern r = Pattern.compile(rp, Pattern.CASE_INSENSITIVE);
            for (String s : uaCase) {
                Matcher m = r.matcher(s);
                if (m.find()) {
                    System.out.println(i + " : " + m.group(0));
                    System.out.println("name : " + m.group(1) + "\n");
                    i++;
                }
            }
            System.out.println("-------------------\n");
        }
    }

    @Test
    public void iosTest() {

        //[]:表示一个字符,该字符可以是[]中指定的内容 [abc]:这个字符可以是a或b或c [a-zA-Z] :表示任意一个字母 [a-zA-Z0-9_] :表示任意一个数字字母下划线 [^abc] :该字符只要不是a或b或c
        //()用于分组,是将括号内的内容看做是一个整体 (abc){3} 表示abc整体出现3次. 可以匹配abcabcabc
        // (abc|def){3} 表示abc或def整体出现3次. 可以匹配: abcabcabc 或 defdefdef 或 abcdefabc 但是不能匹配abcdef 或abcdfbdef
        List<String> pList = Arrays.asList("(ip[honead]+)(?:.*os\\s([\\w.,/\\-]+)\\slike|;\\sopera)",
                "(ip[honead]+).*os\\s([\\w.,/\\-]+)[);]",
                "(ios)\\s([\\w.,/\\-]+)[);]",
                "(ios|ip[honead]+)\\s*([;)])");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 swan/2.26.0 swan-baiduboxapp/12.6.5.10 baiduboxapp/12.6.5.10 (Baidu; P2 14.4.2) ");
        uadd("Mozilla/5.0 (iPhone; U; CPU iPhone OS 5_1_1 like Mac OS X; en-us) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPad; U; iPad OS 5_1_1 like Mac OS X; en-us) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 14,4,2 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 11-6 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0 (iPad; CPU OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15.3.1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 swan/2.26.0 swan-baiduboxapp/12.6.5.10 baiduboxapp/12.6.5.10 (Baidu; P2 14.4.2)");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 MQQBrowser/13.2.0 Mobile/15E148 Safari/604.1 QBWebViewUA/2 QBWebViewType/1");
        uadd("Mozilla/5.0+(iPhone;+CPU+iPhone+OS+15.6.1+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Mobile/15E148+swan/2.26.0+swan-baiduboxapp/12.6.5.10+baiduboxapp/12.6.5.10+(Baidu;+P2+14.4.2)");
        uadd("Mozilla/5.0 (iPad; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0+(iPad;+CPU+OS+15_6_1+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Mobile/15E148");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
//        uadd("");
//        uadd("");

        String ppp = "[^a-zA-Z0-9]";
        //\w 表示任意一个单词字符,等同于[a-zA-Z0-9_]   \W :不是单词字符
        //\s 表示任意一个空白字符  \S :不是空白字符
        //. 表示任意一个字符, 匹配除换行符 \n 之外的任何单字符
        //\d :表示任意一个数字,等同于[0-9]  \D :表示不是数字
        // [honead]+  匹配[]中的一个或多个
        //?  表示前面的内容出现0-1次
        // ?=、?<=、?!、?<! 的使用区别
        // exp1(?=exp2)：查找 exp2 前面的 exp1
        // (?<=exp2)exp1：查找 exp2 后面的 exp1
        //  exp1(?!exp2)：查找后面不是 exp2 的 exp1
        //  (?<!exp2)exp1：查找前面不是 exp2 的 exp1
        //() 表示捕获分组，() 会把每个分组里的匹配的值保存起来， 多个匹配值可以通过数字 n 来查看(n 是一个数字，表示第 n 个捕获组的内容)
        //但用圆括号会有一个副作用，使相关的匹配会被缓存，此时可用 ?: 放在第一个选项前来消除这种副作用
        //+  表示前面的内容最少出现1次
        //* 表示前面的内容出现任意次(0-多次)—匹配内容与+一致，只是可以一次都不写
        //{n}:表示前面的内容出现n次  例如: [abc]{3} 可以匹配:aaa 或 bbb 或 aab 或abc 或bbc 但是不能匹配: aaaa 或 aad
        //{n,m}:表示前面的内容出现最少n次最多m次 例如: [abc]{3,5} 可以匹配:aaa 或 abcab 或者 abcc  但是不能匹配:aaaaaa 或 aabbd
        //{n,}:表示前面的内容出现n次以上(含n次) 例如: [abc]{3,} 可以匹配:aaa 或 aaaaa… 或 abcbabbcbabcba… 但是不能匹配:aa 或 abbdaw…

        //用()分成3组，[honead]+ 表示匹配其中的一个或多个字符， ?:表示消除缓存， . 表示除换行符 \n 之外的任何单字符，*表示前面的内容出现任意次(0-多次)，\s 表示任意一个空白字符 加一个\表转义
        Pattern pattern = Pattern.compile("(ip[honead]+)(?:.*os.([\\w.,/\\-]+).like|;\\sopera)", Pattern.CASE_INSENSITIVE);

        int i = 0;
        for (String s : uaCase) {
            System.out.println(s);
            Matcher matcher = pattern.matcher(s);
            if (matcher.find()) {
                System.out.println(i + " : " + matcher.group(0));
                System.out.println("name : " + matcher.group(1));
                System.out.println("version : " + matcher.group(2).replaceAll(ppp, ".") + "\n");
                i++;
            }
        }
    }

    @Test
    public void allTest() {
        //andriod
        uadd("Mozilla/5.0 (Linux; Android 8.1.0; DUB-AL00 Build/HUAWEIDUB-AL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; Android 9.1.0) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; Android 10.1.0 ;) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0");
        uadd("Mozilla/5.0 (Linux; U; Android 10; zh-CN; HLK-AL00 Build/HONORHLK-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/69.0.3497.100 UWS/3.22.2.28 Mobile Safari/537.36 UCBS/3.22.2.28_210922181100 ChannelId(0) NebulaSDK/1.8.100112 Nebula AlipayDefined(nt:WIFI,ws:360|0|3.0) AliApp(AP/10.2.36.8000) AlipayClient/10.2.36.8000 Language/zh-Hans useStatusBar/true isConcaveScreen/false Region/CNAriver/1.0.0");
        uadd("Mozilla/5.0 (Linux; Android 11; V2055A; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/87.0.4280.141 Mobile Safari/537.36 VivoBrowser/10.3.18.0");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-cn; PEGM00 Build/RKQ1.200903.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/70.0.3538.80 Mobile Safari/537.36 HeyTapBrowser/40.7.31.1");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-CN; Mi 10 Build/RKQ1.200826.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 UCBrowser/13.6.7.1148 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; U; Android 11; zh-cn; Redmi K30 Build/RKQ1.200826.002) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.116 Mobile Safari/537.36 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (Linux; Android 10; HarmonyOS; ELS-AN00; HMSCore 6.2.0.302) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 10; HarmonyOS; ELS-AN01) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 11; NTH-AN00; HMSCore 6.2.0.302) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; Android 10; SEA-AL10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.105 HuaweiBrowser/12.0.1.300 Mobile Safari/537.36");
        uadd("Mozilla/5.0 (Linux; U; Android 9; zh-cn; KB2000 Build/RP1A.201005.001) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/12.1 Mobile Safari/537.36 COVC/045825");
        uadd("Mozilla/5.0 (Linux; Android 9; V1901A Build/P00610; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.27 SP-engine/2.37.0 baiduboxapp/12.28.5.10 (Baidu; P1 9) NABar/1.0");
        uadd("Mozilla/5.0+(Linux;+Android+12;+LGE-AN00+Build/BRAND-MODEL;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/76.0.3809.89+Mobile+Safari/537.36+T7/12.9+SP-engine/2.21.0+matrixstyle/0+lite+baiduboxapp/5.4.0.10+(Baidu;+P1+8.1.0)+NABar/1.0");
        uadd("Mozilla/5.0+(Linux;+Android+9;+RVL-AL09+Build/BRAND-MODEL;+wv)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/76.0.3809.89+Mobile+Safari/537.36+T7/12.9+SP-engine/2.21.0+matrixstyle/0+lite+baiduboxapp/5.4.0.10+(Baidu;+P1+8.1.0)+NABar/1.0");
        uadd("Dalvik/2.1.0 (Linux; U; Android 9; RVL-AL09 Build/HUAWEIRVL-AL09)");
        uadd("Dalvik/2.1.0 (Linux; U; Android 12; LGE-AN00 Build/HONORLGE-AN00)");
        //ios
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 swan/2.26.0 swan-baiduboxapp/12.6.5.10 baiduboxapp/12.6.5.10 (Baidu; P2 14.4.2) ");
        uadd("Mozilla/5.0 (iPhone; U; CPU iPhone OS 5_1_1 like Mac OS X; en-us) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPad; U; iPad OS 5_1_1 like Mac OS X; en-us) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 14,4,2 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 11-6 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3 XiaoMi/MiuiBrowser/15.6.8");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0 (iPad; CPU OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15.3.1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 swan/2.26.0 swan-baiduboxapp/12.6.5.10 baiduboxapp/12.6.5.10 (Baidu; P2 14.4.2)");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 MQQBrowser/13.2.0 Mobile/15E148 Safari/604.1 QBWebViewUA/2 QBWebViewType/1");
        uadd("Mozilla/5.0+(iPhone;+CPU+iPhone+OS+15.6.1+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Mobile/15E148+swan/2.26.0+swan-baiduboxapp/12.6.5.10+baiduboxapp/12.6.5.10+(Baidu;+P2+14.4.2)");
        uadd("Mozilla/5.0 (iPad; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0+(iPad;+CPU+OS+15_6_1+like+Mac+OS+X)+AppleWebKit/605.1.15+(KHTML,+like+Gecko)+Mobile/15E148");
        uadd("Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148");
        uadd("Mozilla/5.0 ");

        int i = 0;
        for (String s : uaCase) {
            System.out.println(i+":"+s);
            System.out.println(ToolUtil.uaAnalysis(s));
            System.out.println("\n");
            i++;
        }
    }

}
