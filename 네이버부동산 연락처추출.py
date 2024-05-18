from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget, QLabel, QLineEdit, QTextEdit, QStatusBar, QTableView
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import time
from PyQt5.QtCore import QAbstractTableModel, Qt, QThread, pyqtSignal

# 데이터 미리보기 표
class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid() and role == Qt.DisplayRole:
            return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None


class DataScraperThread(QThread):
    update_status = pyqtSignal(str)
    update_dataframe = pyqtSignal(pd.DataFrame)

    def __init__(self, driver, url, realtor_data):
        super().__init__()
        self.driver = driver
        self.url = url
        self.realtor_data = realtor_data

    def run(self):
        self.update_status.emit("데이터 수집 중...")
        try:
            if 'complexes' in self.driver.current_url:
                self.scrape_complexes()
            else:
                self.scrape_offices()
            self.realtor_data.drop_duplicates(inplace=True)
            self.update_dataframe.emit(self.realtor_data)
            entries_count = len(self.realtor_data)  # 데이터 프레임의 현재 행 수를 가져옵니다.
            self.update_status.emit(f"데이터 수집 완료. 현재 데이터 수: {entries_count}")
        except Exception as e:
            self.update_status.emit(f"데이터 수집 오류: {str(e)}")
        
    def scrape_offices(self):
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="listContents1"]/div'))
        )
        article_list = self.driver.find_element(By.XPATH, '//*[@id="listContents1"]/div')
        last_height = self.driver.execute_script("return arguments[0].scrollHeight", article_list)
        
        while True:
            self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", article_list)
            time.sleep(0.5)
            new_height = self.driver.execute_script("return arguments[0].scrollHeight", article_list)
            if new_height == last_height:
                break
            last_height = new_height

        articles = self.driver.find_elements(By.CSS_SELECTOR, '.item_inner')
        for article in articles:
            try:
                try:
                    alternative_link = article.find_element(By.CSS_SELECTOR, '.label.label--cp')
                    alternative_link.click()
                except NoSuchElementException:
                    article_link = article.find_element(By.CSS_SELECTOR, '.item_link')
                    article_link.click()
                time.sleep(1)
            except Exception as e:
                self.update_status.emit(f"항목 클릭 오류: {str(e)}")
                continue

            phone_numbers_text = self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(2) > dd')
            phone_numbers = phone_numbers_text.split(',') if phone_numbers_text != '' else []
            mobilephone = ''
            telephone = ''
            for number in phone_numbers:
                if number.startswith('010'):
                    mobilephone = number.strip()
                else:
                    telephone = number.strip()

            realtor_info = {
                '중개소명': self.safe_find_element_text('div.info_agent_title > strong'),
                '대표자명': self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(1) > dd').split('등록번호')[0].strip(),
                '주소': self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(1) > dl > dd'),
                '전화번호': telephone,
                '휴대폰번호': mobilephone,
                '최근 3개월 집주인확인수': self.safe_int_convert(self.safe_find_element_text('div > div.info_agent_wrap > dl.info_agent.info_agent--record > dt > dd').split('건')[0].strip()),
                '매매매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(1) > span.count')),
                '전세매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(2) > span.count')),
                '월세매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(3) > span.count')),
                '단기매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(4) > span.count'))
            }

            self.realtor_data = pd.concat([self.realtor_data, pd.DataFrame([realtor_info])], ignore_index=True)

    def scrape_complexes(self):
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#complexOverviewList > div.list_contents > div.item_area > div'))
        )
        article_list = self.driver.find_element(By.CSS_SELECTOR, '#complexOverviewList > div.list_contents > div.item_area > div')
        last_height = self.driver.execute_script("return arguments[0].scrollHeight", article_list)
        
        while True:
            self.driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", article_list)
            time.sleep(0.5)
            new_height = self.driver.execute_script("return arguments[0].scrollHeight", article_list)
            if new_height == last_height:
                break
            last_height = new_height

        articles = self.driver.find_elements(By.CSS_SELECTOR, '.item_inner')
        for article in articles:
            try:
                try:
                    alternative_link = article.find_element(By.CSS_SELECTOR, '.label.label--cp')
                    alternative_link.click()
                except NoSuchElementException:
                    article_link = article.find_element(By.CSS_SELECTOR, '.item_link')
                    article_link.click()
                time.sleep(1)
            except Exception as e:
                self.update_status.emit(f"항목 클릭 오류: {str(e)}")
                continue

            phone_numbers_text = self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(2) > dd')
            phone_numbers = phone_numbers_text.split(',') if phone_numbers_text != '' else []
            mobilephone = ''
            telephone = ''
            for number in phone_numbers:
                if number.startswith('010'):
                    mobilephone = number.strip()
                else:
                    telephone = number.strip()

            realtor_info = {
                '중개소명': self.safe_find_element_text('div.info_agent_title > strong'),
                '대표자명': self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(1) > dd').split('등록번호')[0].strip(),
                '주소': self.safe_find_element_text('div.info_agent_wrap > dl:nth-child(1) > dl > dd'),
                '전화번호': telephone,
                '휴대폰번호': mobilephone,
                '최근 3개월 집주인확인수': self.safe_int_convert(self.safe_find_element_text('div > div.info_agent_wrap > dl.info_agent.info_agent--record > dt > dd').split('건')[0].strip()),
                '매매매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(1) > span.count')),
                '전세매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(2) > span.count')),
                '월세매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(3) > span.count')),
                '단기매물수': self.safe_int_convert(self.safe_find_element_text('div.article_quantity > a:nth-child(4) > span.count'))
            }

            self.realtor_data = pd.concat([self.realtor_data, pd.DataFrame([realtor_info])], ignore_index=True)

    def safe_find_element_text(self, css_selector, default=''):
        """안전하게 웹 요소에서 텍스트를 검색하여 찾지 못하면 기본값을 반환합니다."""
        try:
            element_text = self.driver.find_element(By.CSS_SELECTOR, css_selector).text
            return element_text.strip() if element_text.strip() != '' else default
        except NoSuchElementException:
            return default

    def safe_int_convert(self, text, default=''):
        """텍스트를 가능한 경우 정수로 변환하고, 그렇지 않은 경우 기본값을 반환합니다."""
        try:
            return int(text)
        except ValueError:
            return default


class WebScraperApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.driver = None
        self.realtor_data = pd.DataFrame(columns=['중개소명', '대표자명', '주소', '전화번호', '휴대폰번호', '최근 3개월 집주인확인수', '매매매물수', '전세매물수', '월세매물수', '단기매물수'])
        self.scraping_in_progress = False
        self.initUI()
    
    def initUI(self):
        self.setWindowTitle('네이버부동산 중개사 데이터 수집기')
        self.setGeometry(100, 100, 800, 600)
        
        # 중앙 위젯 및 레이아웃
        central_widget = QWidget(self)
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 1. URL 입력
        self.url_input = QLineEdit(self)
        default_url = 'https://new.land.naver.com/offices?ms=37.349109,126.7143817,15&a=APTHGJ&b=A1&e=RETAIL'
        self.url_input.setText(default_url)
        layout.addWidget(self.url_input)

        # 2. 브라우저 시작 버튼
        self.start_browser_button = QPushButton('1. 브라우저 시작', self)
        self.start_browser_button.clicked.connect(self.start_browser)
        layout.addWidget(self.start_browser_button)

        # 3. 데이터 수집 버튼
        self.scrape_data_button = QPushButton('2. 데이터 수집 (반복)', self)
        self.scrape_data_button.clicked.connect(self.scrape_data)
        layout.addWidget(self.scrape_data_button)

        # 4. 데이터 미리 보기 영역을 QTableView로 표시
        self.data_preview = QTableView(self)
        layout.addWidget(self.data_preview)

        # 5. 기본 날짜 서식을 사용한 파일 이름 입력
        self.filename_input = QLineEdit(self)
        today_date = datetime.now().strftime("%y%m%d")
        default_filename = f"중개사 데이터_{today_date}.xlsx"
        self.filename_input.setText(default_filename)
        layout.addWidget(self.filename_input)

        # 6. Excel에 저장 버튼
        self.save_data_button = QPushButton('3. 엑셀로 저장', self)
        self.save_data_button.clicked.connect(self.save_to_excel)
        layout.addWidget(self.save_data_button)

        # 7. 브라우저 종료 버튼
        self.quit_browser_button = QPushButton('4. 브라우저 종료', self)
        self.quit_browser_button.clicked.connect(self.quit_browser)
        layout.addWidget(self.quit_browser_button)

        # 상태 표시줄
        self.status_bar = QStatusBar(self)
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("준비 완료")

    def start_browser(self):
        self.status_bar.showMessage("브라우저 시작 중...")
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--start-maximized')
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.get(self.url_input.text())
            self.status_bar.showMessage("브라우저가 URL로 시작되었습니다: " + self.url_input.text())
        except Exception as e:
            self.status_bar.showMessage(f"브라우저 시작 오류: {str(e)}")

    def scrape_data(self):
        if self.scraping_in_progress:
            self.status_bar.showMessage("데이터 수집이 진행 중입니다.")
            return
        self.scraping_in_progress = True
        self.scrape_data_button.setEnabled(False)

        # 드라이버가 초기화되고 준비되었는지 확인
        if self.driver is None:
            self.status_bar.showMessage("브라우저가 초기화되지 않았습니다.")
            return

        # 스크래핑 스레드를 생성하고 필요한 데이터프레임을 전달합니다.
        self.scraping_thread = DataScraperThread(self.driver, self.url_input.text(), self.realtor_data)
        
        # 신호 연결
        self.scraping_thread.update_status.connect(self.status_bar.showMessage)
        self.scraping_thread.update_dataframe.connect(self.update_data_preview)
        self.scraping_thread.finished.connect(self.scrape_finished)
        
        # 스레드 시작
        self.scraping_thread.start()

    def scrape_finished(self):
        self.scraping_in_progress = False
        self.scrape_data_button.setEnabled(True)

    def update_data_preview(self, dataframe):
        # 데이터 미리 보기용 모델 설정
        model = PandasModel(dataframe)
        self.data_preview.setModel(model)
        # 또한 기본 데이터프레임을 업데이트하여 변경 사항을 유지합니다.
        self.realtor_data = dataframe

    def save_to_excel(self):
        self.status_bar.showMessage("엑셀로 저장 중...")
        try:
            self.realtor_data.to_excel(self.filename_input.text(), index=False)
            self.status_bar.showMessage("엑셀 파일 저장 완료: " + self.filename_input.text())
        except Exception as e:
            self.status_bar.showMessage(f"파일 저장 오류: {str(e)}")

    def quit_browser(self):
        self.status_bar.showMessage("브라우저 종료 중...")
        try:
            if self.driver:
                self.driver.quit()
                self.status_bar.showMessage("브라우저가 종료되었습니다.")
        except Exception as e:
            self.status_bar.showMessage(f"브라우저 종료 오류: {str(e)}")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = WebScraperApp()
    ex.show()
    sys.exit(app.exec_())
