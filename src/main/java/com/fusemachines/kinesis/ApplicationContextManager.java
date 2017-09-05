package com.fusemachines.kinesis;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextManager implements ApplicationContextAware{
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext ctx){
    	applicationContext = ctx;
    }

    public static ApplicationContext getAppContext(){
        return applicationContext;
    } 
}