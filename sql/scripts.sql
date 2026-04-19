CREATE TABLE movies (
    movie_id NUMBER PRIMARY KEY,
    title VARCHAR2(255),
    genre VARCHAR2(255),
    overview CLOB,
    search_text CLOB,
    embedding VECTOR(384)
);

select * from movies 

-- Pentru keyword search ca să meargă keyword search cu Oracle, 
-- trebuie să avem un Oracle text index pe coloana search_text, 
-- de tip CONTEXT. Oracle documentează că indexul CONTEXT este 
-- tipul potrivit pentru documente text
--  și că interogările se fac cu operatorul CONTAINS.

CREATE INDEX movies_text_idx
ON movies(search_text)
INDEXTYPE IS CTXSYS.CONTEXT;
