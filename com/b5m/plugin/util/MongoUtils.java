package com.b5m.plugin.util;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

public class MongoUtils
{
    
    private static Map<String, MongoTemplate> templates = new HashMap<String, MongoTemplate>();
    
    @SuppressWarnings("deprecation")
    public static MongoTemplate getTemplateByDatabaseNameByURL(String url, String databaseName)
    {
        if (StringUtils.isBlank(databaseName) || StringUtils.isBlank(url))
        {
            return null;
        }
        MongoTemplate template = templates.get(databaseName);
        if (template == null)
        {
            MongoURI mongoURI = new MongoURI(url);
            Mongo mongo = null;
            try
            {
                mongo = new Mongo(mongoURI);
                mongo.slaveOk();
            }
            catch (MongoException e)
            {
                e.printStackTrace();
            }
            catch (UnknownHostException e)
            {
                e.printStackTrace();
            }
            return new MongoTemplate(mongo, databaseName);
        }
        return template;
    }
}
