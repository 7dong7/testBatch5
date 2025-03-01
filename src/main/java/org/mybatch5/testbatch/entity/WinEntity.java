package org.mybatch5.testbatch.entity;


import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class WinEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String username;
    private Long win;
    private Boolean reward;

    @Builder
    public WinEntity(Long id, String username, Long win, Boolean reward) {
        this.id = id;
        this.username = username;
        this.win = win;
        this.reward = reward;
    }
}
