use kakaobank-pretest;

create table events (
    event_id bigint not null,
    event_timestamp varchar(255) not null,
    service_code varchar(255),
    event_context varchar(255)
);