package com.neo.web;

import java.util.Locale;

import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.neo.domain.User;

@RestController
public class HelloController {
	
    @RequestMapping("/hello")
	public String hello(Locale locale, Model model) {
		return "hello world";
	}

    
    @RequestMapping("/getUser")
    public User getUser() {
    	User user=new User();
    	user.setUserName("小明");
    	user.setPassWord("xxxx");
        return user;
    }
}