-- Giả định dữ liệu đầu vào đã được làm sạch từ Bài 1 (không còn stop words)
-- Cấu trúc raw_data: (id, category, aspect, comment)
data = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:chararray, category:chararray, aspect:chararray, comment:chararray);

-- Tải danh sách từ đã làm sạch từ Bài 1 (giả sử đã lưu ở Bài 1)
cleaned_words = LOAD 'KetQuaBai1' USING PigStorage(',') AS (id:chararray, word:chararray);

-- ==========================================================
-- YÊU CẦU 1: Thống kê tần số từ > 500 lần
-- ==========================================================
group_words = GROUP cleaned_words BY word;
word_counts = FOREACH group_words GENERATE group AS word, COUNT(cleaned_words) AS count;
high_freq_words = FILTER word_counts BY count > 500;

-- ==========================================================
-- YÊU CẦU 2: Thống kê số bình luận theo Category
-- ==========================================================
group_category = GROUP data BY category;
category_counts = FOREACH group_category GENERATE group AS category, COUNT(data) AS num_comments;

-- ==========================================================
-- YÊU CẦU 3: Thống kê số bình luận theo Aspect
-- ==========================================================
group_aspect = GROUP data BY aspect;
aspect_counts = FOREACH group_aspect GENERATE group AS aspect, COUNT(data) AS num_comments;

-- ==========================================================
-- LƯU KẾT QUẢ
-- ==========================================================
STORE high_freq_words INTO 'ThongKe_TuTren500' USING PigStorage(',');
STORE category_counts INTO 'ThongKe_Category' USING PigStorage(',');
STORE aspect_counts   INTO 'ThongKe_Aspect'   USING PigStorage(',');