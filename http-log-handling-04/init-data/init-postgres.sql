CREATE TABLE raw_data (
                          id serial PRIMARY KEY,
                          event_time timestamp,
                          device varchar(20),
                          browser varchar(20),
                          result varchar(20),
                          message varchar(20),
                          duration integer,
                          account_id integer
);

CREATE TABLE fact_tracking_data (
                                    time timestamp,
                                    device integer,
                                    browser integer,
                                    success boolean,
                                    message integer,
                                    total_duration bigint,
                                    event_count integer
);

CREATE TABLE message_dim (
                             id serial PRIMARY KEY,
                             message varchar(20)
);
insert into message_dim (message) values ('ok'), ('out_of_slot'), ('unknown');

CREATE TABLE device_dim (
                            id serial PRIMARY KEY,
                            device_name varchar(20)
);
insert into device_dim (device_name) values ('mobile'), ('desktop'), ('unknown');

CREATE TABLE browser_dim (
                             id serial PRIMARY KEY,
                             browser_name varchar(20)
);
insert into browser_dim (browser_name) values ('chrome'), ('firefox'), ('edge'), ('unknown');