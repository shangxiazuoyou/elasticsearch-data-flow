package com.everflowx.esmigration.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import springfox.documentation.annotations.ApiIgnore;

/**
 * 主页控制器
 * 
 * @author everflowx
 */
@ApiIgnore
@Controller
public class HomeController {
    
    @RequestMapping("/")
    public String home() {
        return "redirect:/monitor.html";
    }
    
    @RequestMapping("/monitor")
    public String monitor() {
        return "redirect:/monitor.html";
    }
}