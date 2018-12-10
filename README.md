# Dependency
Python + konlpy module + MySQL

# Table Setting
```sql
CREATE table USER(  
    name nvarchar(20),  
    jointime datetime,  
    PRIMARY KEY(name)  
);  
CREATE table MESSAGE(  
    id int,  
    msgtext nvarchar(500),  
    sendtime datetime,  
    PRIMARY KEY(id)  
);  
CREATE table KEYWORD(  
    word nvarchar(40),  
    id int,  
    FOREIGN KEY (id) REFERENCES MESSAGE(id),  
    PRIMARY KEY(word, id)  
);  
CREATE table SENDS(  
    sender nvarchar(20),  
    id int,  
    FOREIGN KEY (id) REFERENCES MESSAGE(id)  
);  
CREATE table CONVERSATION(  
    sender nvarchar(20),  
    receiver nvarchar(20),  
    starttime datetime  
);
```
