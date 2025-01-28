**This mini project basically utilizes sparksql on postgres** 

- Important : You may need some knowledge of Apache Spark, JDBC connector to understand the code completely. I say this because I have used it to connect my code run on jupyter notebook to my PGadmin4 (PostgresSQL DB manager). The notebook uploaded will have some lines missing - some lines would have my credentials for the sql server and hence it would be omitted 

You can refer to the screenshots uploaded in the SS folder to understand what this doc explains, 
1. To test the connection between my notebook and PGAdmin4 server I created a table that included a few columns with relevant appropriate datatytpes you can see in the screenshot "1".
2. I then fed my table with the data from a google news feed URL you will find in the notebook I upload with the variable name "rss_url" and you can see this data output in my PGAdmi4 output terminal after I called it in the screenshot "2"
3. I wrote a function to filter out all the recent news posted in the day - basically last 24 hours, you can see the demonstration in screenshots "3" and "3pt2"
4. Just to add more functionality I extended my table to include some news which was not tech related, so i added a new column called "category" where I added 3 types of news which was technology, financial and athletics. The 3 new rss feed url are in the code within a dictionary with the variable "feeds". You can see how my code shows them added in my notebook in screenshot "4pt2" and called in my PGAdmin4 in the screenshot "4"
5. To check how scalable I can make the code, to demonstrate that we can efficiently deleting large amounts of specific records from the database I made a function that would delete all the records with a specfic term. You can find this term in my function "delete_nfl_records()" You can see the function ran perfectly because when I run my SQL query in the screenshot "5" I dont see any output.
