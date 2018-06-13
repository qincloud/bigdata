package com.bigdata.jersey.action;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.bigdata.jersey.model.Person;

@Path("/hello")
public class HelloJersey {

	@GET
	@Produces("text/plain")
	public String hello() {
		return "hello jersey!";
	}

	@GET
	@Path("hello_get")
	@Produces("text/plain")
	public String hello(@QueryParam("name") String name) {
		return "hello jersey!" + name;
	}

	@POST
	@Path("hello_post")
	@Produces(MediaType.APPLICATION_FORM_URLENCODED)
	public String hello_post(@FormParam("id") String id) {
		return "hello jersey!" + id;
	}

	@GET
	@Path("show/{id}")
	@Produces("text/plain")
	public String show(@PathParam("id") String id) {
		return "hello jersey!" + id;
	}
	
	@GET
	@Path("getPerson")
	@Produces(MediaType.APPLICATION_JSON)
	public Person getPerson() {
		Person person = new Person();
		person.setId("1");
		person.setName("廖振钦");
		return person;
	}

}
