package com.elasticsearch.springboot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class ElasticsearchSpringbootApplication {

	@Autowired
	private TransportClient client;

	@RequestMapping("/")
	public String index() {

		return "index";
	}

	@GetMapping("/get/book/novel")
	@ResponseBody
	public ResponseEntity get(@RequestParam(name = "id", defaultValue = "") String id) {
		if (id.isEmpty()) {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		GetResponse result = client.prepareGet("book", "novel", id).get();
		if (!result.isExists()) {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity(result.getSource(), HttpStatus.OK);
	}

	@PostMapping("/add/book/novel")
	@ResponseBody
	public ResponseEntity add(@RequestParam(name = "title") String title, @RequestParam(name = "author") String author,
			@RequestParam(name = "word_count") int word_count,
			@RequestParam(name = "publish_date") @DateTimeFormat(pattern = "yyyy-MM-dd") Date publish_date) {

		try {
			XContentBuilder content = XContentFactory.jsonBuilder().startObject().field("title", title)
					.field("author", author).field("word_count", word_count)
					.field("publish_date", publish_date.getTime()).endObject();

			IndexResponse response = client.prepareIndex("book", "novel").setSource(content).get();
			return new ResponseEntity(response.getId(), HttpStatus.OK);

		} catch (IOException e) {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}

	}

	@GetMapping("/delete/book/novel")
	@ResponseBody
	public ResponseEntity delete(@RequestParam(name = "id", defaultValue = "") String id) {
		DeleteResponse prepareDelete = client.prepareDelete("book", "novel", id).get();
		return new ResponseEntity(prepareDelete.getId().toString(), HttpStatus.OK);
	}

	@PutMapping("/update/book/novel")
	@ResponseBody
	public ResponseEntity update(@RequestParam(name = "title") String title,
			@RequestParam(name = "author") String author, @RequestParam(name = "id") String id) {
		UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);
		try {

			XContentBuilder content = XContentFactory.jsonBuilder().startObject();
			if (title != null) {
				content.field("title", title);
			}
			if (author != null) {
				content.field("author", author);
			}
			content.endObject();
			updateRequest.doc(content);

		} catch (IOException e) {
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}

		try {
			UpdateResponse response = client.update(updateRequest).get();
			return new ResponseEntity(response.getId(), HttpStatus.OK);
		} catch (Exception e) {
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@PostMapping("/query/book/novel")
	@ResponseBody
	public ResponseEntity query(
			@RequestParam(name="title",required=false)String title,
			@RequestParam(name="author",required=false)String author,
			@RequestParam(name="gt_word_count",defaultValue="0")Integer gtWordCount,
			@RequestParam(name="lt_word_count",required=false)Integer ltWordCount
			){
	 
		 BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		 
		 if (title != null) {
			 boolQuery.must(QueryBuilders.matchQuery("title",title));
		  }
			
		 if (author != null) {
			 boolQuery.must(QueryBuilders.matchQuery("author",author));
		 }
		 
		 RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count").from(gtWordCount);
		 
		 if (ltWordCount != null && ltWordCount >=0) {
			rangeQuery.to(ltWordCount);
		 }

		 boolQuery.filter(rangeQuery);
		 
		 SearchRequestBuilder builder = client.prepareSearch("book").setTypes("novel").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		 .setQuery(boolQuery)
		 .setFrom(0)
		 .setSize(10);
		 
		 System.out.println(builder);
		 SearchResponse searchResponse = builder.get();		 
		 ArrayList<Map<String,Object>> list = new ArrayList<Map<String, Object>>();
		
		 for (SearchHit searchHit : searchResponse.getHits()) {
			 list.add(searchHit.getSource());
		}
		 
		return new ResponseEntity<>(list,HttpStatus.OK);
		 
	}

	public static void main(String[] args) {
		SpringApplication.run(ElasticsearchSpringbootApplication.class, args);
	}
}
