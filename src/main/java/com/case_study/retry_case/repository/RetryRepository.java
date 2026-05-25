package com.case_study.retry_case.repository;

import com.case_study.retry_case.dto.Status;
import com.case_study.retry_case.entity.Retry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface RetryRepository extends JpaRepository<Retry, Long> {
    List<Retry> findByStatusAndNextRetryAtBefore(
            Status status,
            LocalDateTime time
    );

    Optional<Integer> findRetryCountByEventId(String eventId);
}
