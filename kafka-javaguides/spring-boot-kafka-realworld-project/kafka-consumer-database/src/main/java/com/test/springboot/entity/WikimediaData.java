package com.test.springboot.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name="wikimedia_recentchange")
@Setter
@Getter
public class WikimediaData {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private long id;
    @Lob
    private String wikiEventData;
}
