package com.rami.rest;

import com.rami.vo.ClientVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Rami Stefanidis on 6/18/2017.
 */
@RestController
public class ClientManagementRestController {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @RequestMapping(value = "/insertClient" , method = RequestMethod.POST)
    ResponseEntity insertclient(@RequestBody final ClientVO clientVO) {
        LOG.info("Insert Client REST invoked clientVO = {}",clientVO);

        return  new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/updateExistingClient" , method = RequestMethod.POST)
    ResponseEntity updateExistingClient(@RequestBody final ClientVO clientVO) {
        LOG.info("updateExistingClient Client REST invoked clientVO = {}",clientVO);

        return  new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/deleteExistingClient" , method = RequestMethod.POST)
    ResponseEntity deleteExistingClient(@RequestBody final int id) {
        LOG.info("updateExistingClient Client REST invoked id = {}",id);

        return  new ResponseEntity(HttpStatus.OK);
    }

}
