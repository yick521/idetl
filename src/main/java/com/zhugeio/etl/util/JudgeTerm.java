package com.zhugeio.etl.util;

import com.zhugeio.etl.id.Config;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hanqi on 18-3-1.
 */
public class JudgeTerm {

    public static void judgeTerm(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    String rawComIdf = Config.getProp(Config.COMPANY_IDENTIFILER()).trim();
                    String pubKey = Config.getProp(Config.PUBLIC_KEY());
                    String license = Config.getProp(Config.LICENSE());
                    String ciphertext1 = AESUtils.aesDncode(license.replace("\\n","\n"));
                    String content = RSAUtils.decryptWithRSA(ciphertext1,RSAUtils.str2PK(pubKey.replace("\\n","\n")));
                    String [] conArr = content.split("\\|");
                    String endDateStr = conArr[conArr.length-1];
                    String decipheringComIdf = conArr[0];
                    if(!rawComIdf.contentEquals(decipheringComIdf)){
                        String t = "The value of company.identifier in config.properties differs from value in license,please verify and modify...";
                        System.err.println(t);
                        String tmp = "config.properties:"+rawComIdf + ":   license:"+decipheringComIdf+":";
                        System.err.println(tmp);
                        System.exit(1);
                    }

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
                    Date endD = simpleDateFormat.parse(endDateStr);
                    long endDTime = endD.getTime();
                    long currentTime = System.currentTimeMillis();

                    if(currentTime > endDTime){
                        String tmp1 = "The cut-off time of ZhugeioETL service is:"+endDateStr+", the license has expired, please apply for lengthening service time.";
                        System.out.println(tmp1);
                        System.exit(1);
                    }else{
                        Thread.sleep(endDTime - currentTime);
                        String tmp1 = "The cut-off time of ZhugeioETL service is:"+endDateStr+", the license has expired, please apply for lengthening service time.";
                        System.out.println(tmp1);
                        System.exit(1);
                    }
                }catch (Exception e){
                    System.err.println("Verification failed in checking the expired time of zhugeio license:");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }).start();
    }
}
