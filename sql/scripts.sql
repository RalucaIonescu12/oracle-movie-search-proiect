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

-- index vectorial Oracle AI Vector Search
-- embedding = coloana de tip VECTOR(384) in care sunt stocate embedding-urile filmelor
-- ORGANIZATION NEIGHBOR PARTITIONS = tipul de vector index IVF
-- IVF grupeaza vectorii in partitii / clustere si cauta mai intai in zonele cele mai promitatoare
-- DISTANCE COSINE = foloseste cosine distance pentru compararea vectorilor
-- WITH TARGET ACCURACY 90 = motorul incearca sa obtina rezultate apropiate de cele exacte,
-- dar mai rapid, acceptand o mica aproximare
-- PARAMETERS(type IVF, NEIGHBOR PARTITIONS 8) = spune explicit ca indexul este IVF
-- si cate partitii vecine sunt folosite in cautare

CREATE VECTOR INDEX movies_vec_ivf_idx
ON movies(embedding)
ORGANIZATION NEIGHBOR PARTITIONS
DISTANCE COSINE
WITH TARGET ACCURACY 90
PARAMETERS (type IVF, NEIGHBOR PARTITIONS 8);