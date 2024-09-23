CREATE TABLE ipl_matches (
  id INT PRIMARY KEY,
  city VARCHAR(50),
  date DATE,
  player_of_match VARCHAR(50),
  venue VARCHAR(100),
  neutral_venue INT,
  team1 VARCHAR(50),
  team2 VARCHAR(50),
  toss_winner VARCHAR(50),
  toss_decision VARCHAR(20),
  winner VARCHAR(50),
  result VARCHAR(20),
  result_margin INT,
  eliminator VARCHAR(10),
  method VARCHAR(20),
  umpire1 VARCHAR(50),
  umpire2 VARCHAR(50)
);

CREATE TABLE ipl_ball (
  id INT,
  inning INT,
  over INT,
  ball INT,
  batsman VARCHAR(50),
  non_striker VARCHAR(50),
  bowler VARCHAR(50),
  batsman_runs INT,
  extra_runs INT,
  total_runs INT,
  is_wicket INT,
  dismissal_kind VARCHAR(50),
  player_dismissed VARCHAR(50),
  fielder VARCHAR(50),
  extras_type VARCHAR(20),
  batting_team VARCHAR(50),
  bowling_team VARCHAR(50)
);


COPY ipl_matches 
FROM 'C:\Program Files\PostgreSQL\16\data\IPL Dataset\IPL Dataset\IPL_matches.csv'
DELIMITER ','
CSV HEADER;


COPY ipl_ball 
FROM 'C:\Program Files\PostgreSQL\16\data\IPL Dataset\IPL Dataset\IPL_Ball.csv'
DELIMITER ','
CSV HEADER;


SELECT * FROM ipl_matches;
SELECT * FROM ipl_ball;

-- BATTERS BIDDING
--  1.  Aggressive batters - Players with High S.R and Faced 500+ balls(SR=(total runs/balls faced)*100) 
SELECT b1.batsman,
	 ROUND((b1.total_runs::DECIMAL/b1.balls_faced)*100, 2) AS strike_rate 
FROM (SELECT batsman,
	CAST(SUM(batsman_runs) AS float) AS total_runs,
	CAST(COUNT(ball) AS float) AS balls_faced
	FROM ipl_ball
	WHERE extras_type!='wides'
	GROUP BY batsman
	) b1
WHERE b1.balls_faced>=500
ORDER BY strike_rate DESC
LIMIT 10;


-- 2. Anchor batsmen - Players with Good Average and Played >2 IPL (Average=total_runs/count(dismissed)), exclude those players who have not been dismissed once
SELECT b1.batsman,
(b1.total_runs: DECIMAL /b1.dismissed) AS average_score,
b1.dismissed
FROM (SELECT b.batsman,
	SUM(b.batsman_runs) AS total_runs,
	SUM(b.is_wicket) AS dismissed,
	COUNT(DISTINCT EXTRACT(year FROM m.date)) AS season
	FROM ipl_ball b
	JOIN ipl_matches m
	USING(id)
	GROUP BY b.batsman
	HAVING SUM(b.is_wicket)>0)b1
WHERE b1.season > 2
ORDER BY average_score DESC
LIMIT 10;


--  3. Hard hitters - Players scored most runs in boundaries and Played >2 IPL
SELECT b1.batsman, b1.sixes, b1.fours, b1.total_runs, (b1.fours*4)+(b1.sixes*6) AS boundry_runs,
	 ROUND(((b1.fours*4)+(b1.sixes*6))*100::DECIMAL/b1.total_runs, 2) AS boundry_percentage, 
	b1.season
FROM (SELECT b.batsman,
	SUM(b.batsman_runs) AS total_runs,
	SUM(CASE WHEN b.batsman_runs=4 THEN 1 ELSE 0 END) AS fours,
	SUM(CASE WHEN b.batsman_runs=6 THEN 1 ELSE 0 END) AS sixes,
	COUNT(DISTINCT EXTRACT(year FROM m.date)) AS season
	FROM ipl_ball b
	JOIN ipl_matches m
	USING(id)
	GROUP BY b.batsman)b1
WHERE b1.season > 2
ORDER BY boundry_runs DESC, boundry_percentage DESC
LIMIT 10;


-- BOWLERS BIDDING
--  1. Economical bowlers - >=500 balls(economy=total runs conceded/total overs bowled)
WITH b1 AS (SELECT bowler,
	COUNT(ball) AS total_balls,
	SUM(total_runs) AS runs_conceded
	FROM ipl_ball
	GROUP BY bowler
	HAVING COUNT(ball)>=500)
SELECT b1.bowler, b1.total_balls, b1.runs_conceded, b1.total_balls/6 AS total_overs,
	ROUND((b1.runs_conceded/(b1.total_balls/6.0):: decimal), 2) AS economy_bowler
FROM b1
ORDER BY economy_bowler
LIMIT 10;


--  2. Best strike rate bowlers - >=500 balls(strike rate=total balls bowled/total wickets taken), run_outs are not by bowler
WITH b1 AS (SELECT bowler,
	COUNT(ball) AS total_balls,
	SUM(is_wicket) AS wickets_taken
	FROM ipl_ball
	WHERE dismissal_kind != 'run out'
	GROUP BY bowler
	HAVING COUNT(ball)>=500)
SELECT b1.bowler, b1.total_balls, b1.wickets_taken,
	ROUND((b1.total_balls::decimal/b1.wickets_taken), 2) AS strike_rate
FROM b1
ORDER BY strike_rate
LIMIT 10;


-- ALL ROUNDERS BIDDING
--  All-Rounders best batting and bowling strike rate - >=500 ballsFaces and >=300 balls bowled (strikeRate=totalBallsBowled/totalWicketsTaken) (SR=(total runs/balls faced)*100) */
SELECT bat.batsman AS all_rounder, bowl.bowler, bat.batting_sr, bowl.bowling_sr
FROM(SELECT batsman,
	ROUND((SUM(batsman_runs)/COUNT(ball)::decimal)*100, 2) AS batting_sr
	FROM ipl_ball
	GROUP BY batsman
	HAVING COUNT(ball)>=500
	ORDER BY batting_sr DESC) AS bat
JOIN
	(SELECT bowler,
	ROUND(COUNT(ball)::decimal/SUM(is_wicket), 2) AS bowling_sr
	FROM ipl_ball
	GROUP BY bowler
	HAVING COUNT(ball)>=300
	ORDER BY bowling_sr) AS bowl
ON bat.batsman=bowl.bowler
LIMIT 10;


-- WICKETKEEPER'S
-- Wicketkeepers should have played at least 2 IPL Seasons (more number of matches played)
-- Wicketkeepers batting strike rate should be high
-- Wicketkeepers bowling strike rate should be low
-- Wicketkeepers with the most dismissals (caught + stumped) 


-- 1. Get the count of cities that have hosted an IPL match
SELECT COUNT(DISTINCT city) AS ipl_host_city
FROM ipl_matches;


--  2. Create table deliveries_v02
CREATE TABLE deliveries_v02 AS
SELECT *,
(CASE WHEN total_runs>=4 THEN 'boundry'
	WHEN total_runs=0 THEN 'dot'
	ELSE 'other' END) AS ball_result
FROM ipl_ball;
----------------------------------------------------------------
SELECT * FROM deliveries_v02;


--  3. Total number of boundaries and dot balls
SELECT ball_result, COUNT(ball_result) AS total
FROM deliveries_v02
WHERE ball_result IN ('boundry', 'dot')
GROUP BY ball_result;


--  4. Total number of boundaries scored by each team
SELECT batting_team, COUNT(ball_result) AS total_boundries
FROM deliveries_v02
WHERE ball_result='boundry'
GROUP BY batting_team
ORDER BY total_boundries DESC;


--  5. Total number of dot balls bowled by each team
SELECT bowling_team, COUNT(ball_result) AS total_dot_balls
FROM deliveries_v02
WHERE ball_result='dot'
GROUP BY bowling_team
ORDER BY total_dot_balls DESC;


--  6. Total number of dismissals by dismissal kinds where dismissal kind is not NA
SELECT dismissal_kind, COUNT(dismissal_kind) AS total_dismissal
FROM deliveries_v02
WHERE dismissal_kind!='NA'
GROUP BY dismissal_kind;


--  7. Top 5 bowlers who conceded maximum extra runs from deliveries table(i.e., ipl_ball) 
SELECT bowler, SUM(extra_runs) AS max_extra_runs
FROM ipl_ball
GROUP BY bowler
ORDER BY max_extra_runs DESC
LIMIT 5;


--  8. Create a table named deliveries_v03
CREATE TABLE deliveries_v03 AS
	(SELECT d.*, 
		m.venue AS venue, 
		m.date AS match_date
	FROM deliveries_v02 AS d
	LEFT JOIN ipl_matches AS m
	USING(id));
------------------------------------------------
SELECT * FROM deliveries_v03;


--  9. Total runs scored for each venue
SELECT venue, SUM(total_runs) AS total_runs
FROM deliveries_v03
GROUP BY venue
ORDER BY total_runs DESC;


--  10. year-wise total runs scored at Eden Gardens
SELECT DISTINCT EXTRACT(year FROM match_date) AS years,
	SUM(total_runs) AS Eden_Gardens_total_runs
FROM deliveries_v03
WHERE venue ='Eden Gardens'
GROUP BY years
ORDER BY Eden_Gardens_total_runs DESC;

