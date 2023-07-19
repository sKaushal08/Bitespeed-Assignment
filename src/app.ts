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
        'primaryContactId': primaryContactId,
        'emails': emailResult,
        'phoneNumbers': phoneNumberResult,
        'secondaryContactIds': JSON.parse(secondaryContactIds) as number[]
    }
}



const app = express();

app.get('/', (req: any, res: { send: (arg0: string) => void; }) => {
    res.send('Hello, World!');
});
app.use(express.json());

app.post('/identify', (req, res)=>{
    const {email, phoneNumber} = req.body;
    const query = `SELECT * FROM Contact WHERE email='${email}' OR phoneNumber=${phoneNumber} ORDER BY id`
    const resultQuery = `SELECT 
        c1.id AS primaryContactId, 
        JSON_ARRAYAGG(c2.email) AS emails, 
        JSON_ARRAYAGG(c2.phoneNumber) AS phoneNumbers, 
        JSON_ARRAYAGG(c2.id) AS secondaryContactIds 
    FROM  Contact c1 LEFT JOIN  Contact c2 ON (c2.linkedId = c1.id or c2.id = c1.id) 
    WHERE c1.email = '${email}' or c1.phoneNumber = ${phoneNumber} 
    GROUP BY c1.id;`
    
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
        connection.query(`SELECT COUNT(*) - 2 AS count FROM Contact WHERE email = '${email}' OR phoneNumber = ${phoneNumber}`, (err: any, results: any) => {
            console.log('Count Query start', results[0]['count']);
            callback(results[0]['count']);
        });
    };

    connection.query(query, (err: any, results: any) => {
        if (err) {
            console.error('Error executing the query:', err);
        } else if(results.length == 0){
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence) VALUES ('${email}', ${phoneNumber}, 'primary')`)
        } else {
            connection.query(`INSERT INTO Contact (email, phoneNumber, linkPrecedence, linkedId) VALUES ('${email}', ${phoneNumber}, 'secondary', ${results[0].linkPrecedence == 'primary' ? results[0].id : results[0].linkedId})`)
        }
    });

    getCountToUpdate((count: any)=>{
        if (count != null && count>=0){
            connection.query(`WITH headSubquery AS (
                SELECT 
                CASE
                    WHEN (SELECT linkedId
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = ${phoneNumber}) AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery) IS NULL
                    THEN (SELECT id as variable
                          FROM (SELECT DISTINCT id
                                FROM Contact
                                WHERE email = "${email}" OR phoneNumber = ${phoneNumber}
                                ORDER BY id
                                LIMIT 1) AS subquery)
                    ELSE (SELECT linkedId as variable
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = ${phoneNumber}) AND linkedId is not null
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
                        WHERE (email = "${email}" OR phoneNumber = ${phoneNumber})
                        ORDER BY linkedId
                    ) AS subquery
                )
                OR 
                ((email = "${email}" OR phoneNumber = ${phoneNumber}) AND 
                (linkPrecedence = 'primary' AND id != (select variable from headSubquery))
            ));`, (err: any, results: any) => {
                console.log('Secondary Update Query start');
                if (err) {
                    console.error('Error executing the query:', err);
                    return
                } else {
                    console.log('update query results', results);
                }
                console.log('Secondary Update Query end', count);
            });

            connection.query(`UPDATE Contact SET linkPrecedence = 'secondary' 
            WHERE (email = "${email}" OR phoneNumber = ${phoneNumber})
            AND (linkPrecedence = 'primary' AND id != (
                SELECT 
                CASE
                    WHEN (SELECT linkedId
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = ${phoneNumber}) AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery) IS NULL
                    THEN (SELECT id as variable
                          FROM (SELECT DISTINCT id
                                FROM Contact
                                WHERE email = "${email}" OR phoneNumber = ${phoneNumber}
                                ORDER BY id
                                LIMIT 1) AS subquery)
                    ELSE (SELECT linkedId as variable
                        FROM (SELECT DISTINCT linkedId
                            FROM Contact
                            WHERE (email = "${email}" OR phoneNumber = ${phoneNumber}) AND linkedId is not null
                            ORDER BY linkedId
                            LIMIT 1) AS subquery)
                END
            ));`, (err: any, results: any) => {
                console.log('Second Secondary Update Query start');
                if (err) {
                    console.error('Error executing the query:', err);
                    return
                } else {
                    console.log('update query results', results);
                }
                console.log('Second Secondary Update Query end');
            });
        }
        console.log('Count Query end', count);
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
