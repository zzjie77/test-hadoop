package com.netvour.hadoop;

import java.io.IOException;
import java.util.Properties;

/**
 * User: zhangjunjie
 * Date: 14-6-9
 * Time: 下午12:43
 */
public class PropertiesUtil {

    private static Properties dataSourceProp ;

    public synchronized static Properties getDataSourceProp(){
        try {
            if(dataSourceProp == null) {
                dataSourceProp = new Properties();
                dataSourceProp.load(PropertiesUtil.class.getClassLoader().getResourceAsStream("datasource.properties"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataSourceProp;
    }


}
