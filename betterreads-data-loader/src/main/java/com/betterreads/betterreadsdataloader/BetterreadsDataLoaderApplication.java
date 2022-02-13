package com.betterreads.betterreadsdataloader;

import com.betterreads.betterreadsdataloader.author.Author;
import com.betterreads.betterreadsdataloader.author.AuthorRepository;
import com.betterreads.betterreadsdataloader.book.Book;
import com.betterreads.betterreadsdataloader.book.BookRepository;
import connection.DataStaxAstraProperties;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.authors}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));

					log.info("Saving {} to db.", author);
					authorRepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void initWorks() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		Path path = Paths.get(worksDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));

					JSONObject descriptionObj = jsonObject.optJSONObject("description");
					if(!Objects.isNull(descriptionObj)) {
						book.setDescription(descriptionObj.optString("value"));
					}

					JSONObject createdObj = jsonObject.optJSONObject("created");
					if(!Objects.isNull(createdObj)) {
						String dateStr = createdObj.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, formatter));
					}

					JSONArray coverJsonArr = jsonObject.optJSONArray("covers");
					if(!Objects.isNull(coverJsonArr)) {
						List<String> coverIds = new ArrayList<>();
						for(int i=0; i<coverJsonArr.length(); i++) {
							coverIds.add(coverJsonArr.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorJsonArr = jsonObject.optJSONArray("authors");
					if(!Objects.isNull(authorJsonArr)) {
						List<String> authorIds = new ArrayList<>();
						for(int i=0; i<authorJsonArr.length(); i++) {
							String authorId = authorJsonArr.getJSONObject(i).getJSONObject("author").getString("key")
										.replace("/authors /", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);

						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
									.map(optionalAuthor -> {
										if (optionalAuthor.isEmpty()) {
											return "Unknown Author";
										}
										return optionalAuthor.get().getName();
									}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}

					log.info("Saving book {} to db", book.getName());
					bookRepository.save(book);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthors();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}
}
