# movieLensRecommender

包含將近3900部電影共1000209筆匿名評等
其中為2000年加入MovieLens 6040 MovieLens用戶的觀看資料。

movielens 1M data
共有3900部電影和6040的用戶的相關評分共計1000209筆
資料分為3部分:
   
        I. ratings.dat 
	  1. 資料格式: userID::movieID::rating::timestamp
	  2. rating為五級距
	  3. timestamp為自1970/1/1開始的秒數
	  4. 每個用戶都至少有20個評分(已經整理過，real data此點為需要克服的問題之一)
	
	II. users.dat
	  1. 資料格式: userID::gender::age::occupation::zip-code
	  2. Gender 分為"M"男生和"F"女生
	  3. 年齡分為7個級距分別以該級距最小年齡作為紀錄，如下列
	  	1:  "Under 18"
	    18:  "18-24"
	    25:  "25-34"
	    35:  "35-44"
	    45:  "45-49"
	    50:  "50-55"
	    56:  "56+"
	  4. 職業共有21個類別，如下列
	    0:  "other" or not specified
		1:  "academic/educator"
		2:  "artist"
		3:  "clerical/admin"
		4:  "college/grad student"
		5:  "customer service"
		6:  "doctor/health care"
		7:  "executive/managerial"
		8:  "farmer"
		9:  "homemaker"
		10:  "K-12 student"
		11:  "lawyer"
		12:  "programmer"
		13:  "retired"
		14:  "sales/marketing"
		15:  "scientist"
		16:  "self-employed"
		17:  "technician/engineer"
		18:  "tradesman/craftsman"
		19:  "unemployed"
		20:  "writer"
	
	III. movies.dat
	  1. 資料格式: MovieID::Title::Genres
	  2. 共分成18個類別(參考IMDB)，一部電影可有多個類別用"|"隔開，如下列
		Action
		Adventure
		Animation
		Children's
		Comedy
		Crime
		Documentary
		Drama
		Fantasy
		Film-Noir
		Horror
		Musical
		Mystery
		Romance
		Sci-Fi
		Thriller
		War
		Western
	  3.電影資料為人工輸入，應進行基本防呆	
	  
參考來源: http://grouplens.org/datasets/movielens/1m/	  

目前建構方法: ALS-WR, user-based, movie association recommender

卡方值計算可參考: http://ocw.jhsph.edu/courses/fundepiii/pdfs/lecture17.pdf

