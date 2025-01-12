--------------------------------------------------------
--  File created - Monday-January-13-2025   
--------------------------------------------------------
DROP PACKAGE BODY "PROJ_USER"."USER_WISE_STOCK_PKG";
--------------------------------------------------------
--  DDL for Package USER_WISE_STOCK_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PROJ_USER"."USER_WISE_STOCK_PKG" AS 
  PROCEDURE CSV_TO_TAB(DIR_PATH VARCHAR2);
END USER_WISE_STOCK_PKG;

/
--------------------------------------------------------
--  DDL for Package Body USER_WISE_STOCK_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PROJ_USER"."USER_WISE_STOCK_PKG" AS

  PROCEDURE CSV_TO_TAB(DIR_PATH VARCHAR2) AS
  exchange_file utl_file.file_type;
  depository_file utl_file.file_type;
  csv_rec varchar2(32767);
  tran_val varchar2(32767);
  is_tab PLS_INTEGER;
  sql_query varchar2(500);
  ins_query varchar2(1000);
  V_error_code NUMBER;
  V_error_message VARCHAR2(255);
  BEGIN
    /* TO CHECK IF BOTH TABLE EXIST THEN TRUNCATE FOR INSERT NEW DATA OTHERWISE CREATE TABLE  */
        select COUNT(*) INTO is_tab from user_tables where table_name=UPPER('exchange');
        IF is_tab >0 THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE exchange';
        ELSE
            sql_query := 'create table exchange
                            (User_ID varchar2(50),
                            Stock_ID varchar2(50),
                            Stock_Name varchar2(100),
                            Stock_Count varchar2(50),
                            create_on_date date default sysdate
                            )';
            execute IMMEDIATE sql_query;
        END IF;    
        select COUNT(*) INTO is_tab from user_tables where table_name=UPPER('depository');
        IF is_tab >0 THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE depository';
        ELSE
            sql_query := 'create table depository as 
                          select * from exchange where 1=2';
             execute IMMEDIATE sql_query;
        END IF;    
 ---------- READ DATA FROM CSV AND INSERT IN TO TABLE
 
    exchange_file :=  utl_file.fopen(DIR_PATH,'exchange_data.csv','r');
    depository_file :=  utl_file.fopen(DIR_PATH,'depository_data.csv','r');
    utl_file.get_line(exchange_file,csv_rec);  -- SKIP HEADER FOR exchange_file
    utl_file.get_line(depository_file,csv_rec);   -- SKIP HEADER FOR depository_file
    LOOP
        /* READ DATA FROM exchange_file AND LOAD INTO TABLE EXCHANGE */
        BEGIN
            utl_file.get_line(exchange_file,csv_rec);
            select ''''||replace(csv_rec,',',''',''')||''''  into tran_val from dual;
            ins_query :='INSERT INTO EXCHANGE (User_ID,Stock_ID,Stock_Name,Stock_Count) VALUES ('||tran_val||')';
            execute IMMEDIATE ins_query;
        EXCEPTION when no_data_found then   exit;
        END;
        /* READ DATA FROM depository_file AND LOAD INTO TABLE DEPOSITORY */
        BEGIN
            utl_file.get_line(depository_file,csv_rec);
            select ''''||replace(csv_rec,',',''',''')||''''  into tran_val from dual;
            ins_query :='INSERT INTO DEPOSITORY (User_ID,Stock_ID,Stock_Name,Stock_Count) VALUES ('||tran_val||')';
            execute IMMEDIATE ins_query;
        EXCEPTION when no_data_found THEN  exit;
        END;
    END LOOP;
   
    utl_file.fclose(exchange_file);
    utl_file.fclose(depository_file);
  ------------------ STORING OUTCOM INTO LOG TABLE  
    select COUNT(*) INTO is_tab from user_tables where table_name=UPPER('OUTCOM_LOG');
    IF is_tab >0 THEN
        EXECUTE IMMEDIATE 'DROP TABLE OUTCOM_LOG';
    END IF;  
    ins_query :='CREATE TABLE OUTCOM_LOG AS
                 SELECT dp.USER_ID , dp.STOCK_NAME, 
                        case when dp.STOCK_ID<>ex.STOCK_ID then ''mismatches'' ELSE ''matches'' END STCK_ID_STATUS
                        ,dp.STOCK_COUNT DEPOSITORY_COUNT,ex.STOCK_COUNT EXCHANGE_COUNT, 
                        case when dp.STOCK_COUNT<>ex.STOCK_COUNT then ''mismatches'' ELSE ''matches'' END STCK_COUNT_STATUS  
                 FROM DEPOSITORY dp 
                 FULL OUTER JOIN EXCHANGE ex 
                            ON ex.user_id=dp.user_id AND ex.STOCK_id=dp.STOCK_id';
        --DBMS_OUTPUT.PUT_LINE(ins_query);
        EXECUTE IMMEDIATE ins_query;

     EXCEPTION
            WHEN UTL_FILE.INVALID_OPERATION THEN
                 UTL_FILE.FCLOSE(exchange_file);
                 UTL_FILE.FCLOSE(depository_file);
                dbms_output.put_line('File could not be opened or operated on as requested.');   
            WHEN OTHERS THEN
               v_error_code := SQLCODE;
               v_error_message := SQLERRM;
              dbms_output.put_line(v_error_code || SQLERRM);
 END CSV_TO_TAB;
-------------------------------
END USER_WISE_STOCK_PKG;

/
