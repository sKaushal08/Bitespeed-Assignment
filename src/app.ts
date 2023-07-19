import express from 'express';
import { createConnection } from 'mysql';

const connection = createConnection({
    host: 'database-1.cykz2htoubcv.ap-south-1.rds.amazonaws.com',
    port: 3306,
    user: 'root',
    password: 'Qwerty1234',
    database: 'bitespeed'
  });
  
connection.connect((err: any) => {
    if (err) {
        console.error('Error connecting to the database:', err);
        return;
    }
    console.log('Connected to Amazon RDS');
});

const executeQuery = (query: string): Promise<any[]> => {
    return new Promise((resolve, reject) => {
      connection.query(query, (error, results) => {
        if (error) {
          console.error('Error executing the query:', error);
          reject(error);
        } else {
          resolve(results);
        }
      });
    });
  };

const processQueryResult = (queryResult:any) => {
    const {primaryContactId, emails, phoneNumbers, secondaryContactIds} = queryResult;
    
    const emailResult: string[] = [];
    for (let email of JSON.parse(emails) as string[]){        
        if (!emailResult.includes(email)){
            emailResult.push(email)
        }
    }
    const phoneNumberResult: number[] = [];
    for (let phoneNumber of JSON.parse(phoneNumbers) as number[]){
        if (!phoneNumberResult.includes(phoneNumber)){
            phoneNumberResult.push(phoneNumber)
        }
    }
    return {
        'primaryContactId': primaryContactId as number,
        'emails': emailResult,
        'phoneNumbers': phoneNumberResult,
        'secondaryContactIds': JSON.parse(secondaryContactIds).filter((item: any) => item != primaryContactId) as number[]
    }
}



const app = express();
app.use(express.json());

app.post('/identify', async(req, res)=>{
    const {email, phoneNumber} = req.body;
    const query = `SELECT * FROM Contact WHERE email='${email}' OR phoneNumber="${phoneNumber}" ORDER BY id`
    
    const resultQuery = `SELECT c1.id AS primaryContactId, 
        JSON_ARRAYAGG(c2.email) AS emails, 
        JSON_ARRAYAGG(c2.phoneNumber) AS phoneNumbers, 
        JSON_ARRAYAGG(c2.id) AS secondaryContactIds 
    FROM  Contact c1 LEFT JOIN Contact c2 ON c1.id = c2.linkedId or c1.id = c2.id
    WHERE (c1.email = '${email}' OR c1.phoneNumber = '${phoneNumber}') OR c1.linkedId = (SELECT linkedId From Contact Where email = '${email}' OR phoneNumber = "${phoneNumber}" Limit 1)
    GROUP BY c1.linkedId;`
    
    const getIdentity = (callback: any) => {
        connection.query(resultQuery, (error, response) => {
            console.log('Result Query start');
            if (error) {
                console.error('Error executing the query:', error);
                callback(error, null);
            } else {
                callback(null, response);
            }
            console.log('Result Query end');
        });
    };

    const getCountToUpdate = (callback: any) => {
        connection.query(`SELECT COUNT(*) - 2 AS count FROM Contact WHERE email = '${email}' OR phoneNumber = "${phoneNumber}"`, (err: any, results: any) => {
            console.log('Count Query start', results[0]['count']);
            callback(results[0]['count']);
        });
    };

    const results: any[] = await executeQuery(query);
    if (results.length == 0) {
      await executeQuery(
        `INSERT INTO Contact (email, phoneNumber, linkPrecedence) VALUES ('${email}', "${phoneNumber}", 'primary')`
      );
    } else {
      await executeQuery(
        `INSERT INTO Contact (email, phoneNumber, linkPrecedence, linkedId) VALUES ('${email}', "${phoneNumber}", 'secondary', ${results[0].linkPrecedence == 'primary' ? results[0].id : results[0].linkedId})`
      );
    }

    getCountToUpdate(async(count: any)=>{
        if (count != null && count>=0){
            await executeQuery(`WITH headSubquery AS (
                SELECT 
                CASE
                    WHEN (SELECT linkedId
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}") AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery) IS NULL
                    THEN (SELECT id as variable
                          FROM (SELECT DISTINCT id
                                FROM Contact
                                WHERE email = "${email}" OR phoneNumber = "${phoneNumber}"
                                ORDER BY id
                                LIMIT 1) AS subquery)
                    ELSE (SELECT linkedId as variable
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}") AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery)
                END as variable
            )
            
            UPDATE Contact
            SET linkedId = (
                Select variable FROM headSubquery
            )
            WHERE (
                linkedId IN (
                    SELECT linkedId
                    FROM (
                        SELECT id as linkedId
                        FROM Contact
                        WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}")
                        
                        UNION
                        
                        SELECT linkedId
                        FROM Contact
                        WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}")
                    ) AS subquery
                    ORDER BY linkedId
                OR id IN (
                    SELECT id
                    FROM (
                        SELECT id
                        FROM Contact
                        WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}")
                        
                        UNION
                        
                        SELECT linkedId as id
                        FROM Contact
                        WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}")
                    ) AS subquery
                    )
                )
                OR 
                ((email = "${email}" OR phoneNumber = "${phoneNumber}") AND 
                (linkPrecedence = 'primary' AND id != (select variable from headSubquery))
            ));`);

            await executeQuery(`UPDATE Contact SET linkPrecedence = 'secondary' 
            WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}")
            AND (linkPrecedence = 'primary' AND id != (
                SELECT 
                CASE
                    WHEN (SELECT linkedId
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}") AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery) IS NULL
                    THEN (SELECT id as variable
                          FROM (SELECT DISTINCT id
                                FROM Contact
                                WHERE email = "${email}" OR phoneNumber = "${phoneNumber}"
                                ORDER BY id
                                LIMIT 1) AS subquery)
                    ELSE (SELECT linkedId as variable
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = "${phoneNumber}") AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery)
                END
            ));`);
        }
    });
    


    getIdentity((error: any, identity: any) => {
        let result = {};
        if (error) {
            console.error('Error retrieving identity:', error);
        } else if(identity[0]!= undefined){
            result = processQueryResult(identity[0]);
        }
        res.json({'contact': result});
    });    
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
