"""
Парсер для сайта IrCity.ru
"""
import re
import json
import asyncio
from typing import Optional
from datetime import datetime

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from ..base_news import BaseNewsParser
from database.models import Post
from utils.helpers import clean_text, generate_hash


# Словарь русских месяцев
RUSSIAN_MONTHS = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
    'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
    'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
}


def parse_russian_date_with_time(date_str: str) -> Optional[datetime]:
    """Парсинг даты в формате '13 января, 2026, 13:10' или '13 января 2026'"""
    if not date_str:
        return None

    # Формат с временем: "13 января, 2026, 13:10"
    pattern_with_time = r'(\d{1,2})\s+(\w+),?\s+(\d{4}),?\s+(\d{1,2}):(\d{2})'
    match = re.search(pattern_with_time, date_str.lower())

    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        year = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))

        month = RUSSIAN_MONTHS.get(month_name)
        if month:
            try:
                return datetime(year, month, day, hour, minute)
            except ValueError:
                return None

    # Формат без времени: "13 января 2026"
    pattern_no_time = r'(\d{1,2})\s+(\w+)\s+(\d{4})'
    match = re.search(pattern_no_time, date_str.lower())

    if match:
        day = int(match.group(1))
        month_name = match.group(2)
        year = int(match.group(3))

        month = RUSSIAN_MONTHS.get(month_name)
        if month:
            try:
                return datetime(year, month, day)
            except ValueError:
                return None

    return None


class IrCityParser(BaseNewsParser):
    """
    Парсер для сайта IrCity.ru

    Особенности:
    - URL статей: /text/{category}/{year}/{month}/{day}/{article-id}/
    - Раздел туризма: /text/tags/turizm/
    - Сайт использует CSS-модули с хэшированными классами (articleContent_XXXXX)
    - Дата извлекается из URL или JSON-LD
    """

    # Паттерн для поиска ссылок на статьи
    LINK_PATTERN = re.compile(r'/text/\w+/\d{4}/\d{2}/\d{2}/\d+/')

    def __init__(self, source_id: str):
        super().__init__(
            source_id=source_id,
            source_name="IrCity.ru",
            base_url="https://ircity.ru"
        )
        self.tourism_section = "/text/tags/turizm/"
        self.skip_relevance_check = False

    async def _fetch_with_curl(self, session: AsyncSession, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """Загрузить страницу через curl_cffi (Chrome TLS fingerprint) с retry."""
        last_error = None
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = 2 ** attempt
                    self.log_debug(f"Повторная попытка #{attempt + 1} после {delay}s для {url}")
                    await asyncio.sleep(delay)
                response = await session.get(url, timeout=30)
                if response.status_code == 200:
                    return BeautifulSoup(response.text, "lxml")
                if response.status_code in (403, 428, 429):
                    last_error = f"HTTP {response.status_code}"
                    self.log_debug(f"HTTP {response.status_code} для {url} (попытка {attempt + 1}/{max_retries})")
                    continue
                self.log_debug(f"HTTP {response.status_code} для {url}")
                return None
            except Exception as e:
                last_error = str(e)
                self.log_error(f"Ошибка загрузки {url}: {e} (попытка {attempt + 1}/{max_retries})")

        if last_error:
            self.log_error(f"Не удалось загрузить {url} после {max_retries} попыток. Последняя ошибка: {last_error}")
        return None

    async def _parse_section(self) -> list[Post]:
        """Парсинг раздела туризма"""
        posts = []
        url = f"{self.base_url}{self.tourism_section}"

        # curl_cffi с Chrome TLS impersonation для обхода DDoS-Guard
        # Cookies, заголовки и JA3-fingerprint как у настоящего Chrome.
        async with AsyncSession(impersonate="chrome131") as session:
            # Прогрев: запрос на главную, чтобы DDoS-Guard выдал cookie
            try:
                warmup = await session.get(self.base_url, timeout=30)
                self.log_debug(f"Прогрев DDoS-Guard: HTTP {warmup.status_code}")
                await asyncio.sleep(2)
            except Exception as e:
                self.log_debug(f"Ошибка при прогреве: {e}")

            soup = await self._fetch_with_curl(session, url)
            if not soup:
                self.log_info("⚠️ Не удалось получить данные (возможна блокировка DDoS-Guard)")
                return posts

            links = await self._get_article_links(soup)
            self.log_info(f"Найдено {len(links)} ссылок на статьи")

            for link in links[:self.max_posts]:
                article_url = self._make_absolute_url(link)
                article_soup = await self._fetch_with_curl(session, article_url)

                if article_soup:
                    post = await self._parse_article(article_soup, article_url, section_soup=soup)
                    if post:
                        posts.append(post)
                        await self.delay()

        return posts

    async def _get_article_links(self, soup: BeautifulSoup) -> list[str]:
        """Получить ссылки на статьи из контейнера announcementList"""
        links = []
        seen = set()

        # Ищем контейнер со списком статей
        announcement_list = soup.find(class_=re.compile(r'announcementList'))
        if announcement_list:
            # Ищем ссылки только внутри этого контейнера
            for a_tag in announcement_list.find_all('a', href=self.LINK_PATTERN):
                href = a_tag.get('href', '')
                # Исключаем страницы комментариев и обсуждений
                if href and href not in seen:
                    # Проверяем что это не комментарии
                    if not href.endswith('/comments/') and '/comments/' not in href and '?discuss=' not in href:
                        seen.add(href)
                        links.append(href)
        else:
            # Fallback: ищем везде
            for a_tag in soup.find_all('a', href=self.LINK_PATTERN):
                href = a_tag.get('href', '')
                if href and href not in seen:
                    if not href.endswith('/comments/') and '/comments/' not in href and '?discuss=' not in href:
                        seen.add(href)
                        links.append(href)

        return links

    async def _parse_article(self, soup: BeautifulSoup, url: str, section_soup: BeautifulSoup) -> Optional[Post]:
        """Парсинг статьи"""
        title = self._extract_title(soup)
        if not title:
            self.log_debug(f"Не найден заголовок для {url}")
            return None
        
        published_at = self._extract_date(soup, url)

        # Извлекаем количество просмотров и комментариев
        views_count = self._extract_views_count(soup)
        comments_count = self._extract_comments_count(section_soup, url)

        content = self._extract_content(soup)
        if not content:
            content = title


        if not self._is_recent(published_at):
            self.log_debug(f"Статья слишком старая: {published_at}")
            return None


        return Post(
            source_id=self.source_id,
            external_id=generate_hash(url),
            title=title,
            content=content,
            url=url,
            published_at=published_at,
            views_count=views_count,
            comments_count=comments_count,
        )

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечь заголовок"""
        # Пробуем JSON-LD сначала (самый надежный источник)
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get('headline'):
                    return clean_text(data['headline'])
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('headline'):
                            return clean_text(item['headline'])
            except (json.JSONDecodeError, TypeError):
                pass

        # Пробуем h1
        h1 = soup.find('h1')
        if h1:
            title = clean_text(h1.get_text())
            if title:
                return title

        # Пробуем meta og:title
        og_title = soup.find('meta', property='og:title')
        if og_title and og_title.get('content'):
            return clean_text(og_title['content'])

        return ""

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Извлечь контент статьи"""
        # Удаляем все мусорные элементы
        # CSS-модули используют паттерн classname_hash
        junk_patterns = [
            # Навигация и структура
            re.compile(r'gridAside'),
            re.compile(r'sidebar'),
            re.compile(r'header'),
            re.compile(r'footer'),
            re.compile(r'menu'),
            re.compile(r'nav'),
            re.compile(r'breadcrumb'),
            # Списки статей (самое важное для IrCity!)
            re.compile(r'announcementList'),
            re.compile(r'articleList'),
            re.compile(r'newsList'),
            # Реклама и соцсети
            re.compile(r'banner'),
            re.compile(r'advert'),
            re.compile(r'social'),
            re.compile(r'share'),
            # Интерактивные элементы
            re.compile(r'button'),
            re.compile(r'subscribe'),
            re.compile(r'commentForm'),
            re.compile(r'comment'),  # Блоки комментариев
            # Метаинформация
            re.compile(r'tags'),
            re.compile(r'category'),
            re.compile(r'related'),
            re.compile(r'author'),
            re.compile(r'date'),
            re.compile(r'source'),  # Источники
        ]

        for pattern in junk_patterns:
            for elem in soup.find_all(class_=pattern):
                elem.decompose()

        # Удаляем скрипты и стили
        for tag in soup.find_all(['script', 'style', 'noscript', 'iframe']):
            tag.decompose()

        # Удаляем заголовки из контента (они уже в поле title)
        for h in soup.find_all(['h1', 'h2']):
            h.decompose()

        # Ищем контент по паттерну CSS-модулей
        content_elem = soup.find(class_=re.compile(r'articleContent'))
        if content_elem:
            # Удаляем текстовые блоки с источниками
            for elem in content_elem.find_all(string=re.compile(r'Источник:', re.IGNORECASE)):
                try:
                    if elem.parent and hasattr(elem.parent, 'decompose'):
                        elem.parent.decompose()
                except (AttributeError, TypeError):
                    continue

            text = clean_text(content_elem.get_text())
            if len(text) > 100:
                return text

        return ""

    def _extract_date(self, soup: BeautifulSoup, url: str) -> Optional[datetime]:
        """Извлечь дату публикации с временем"""
        # Пробуем JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    date_str = data.get('datePublished', '')
                    if date_str:
                        # Убираем таймзону и парсим
                        date_str = date_str.split('+')[0].split('Z')[0]
                        return datetime.fromisoformat(date_str)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('datePublished'):
                            date_str = item['datePublished'].split('+')[0].split('Z')[0]
                            return datetime.fromisoformat(date_str)
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

        # Пробуем time элемент с datetime атрибутом
        time_elem = soup.find("time")
        if time_elem and time_elem.get('datetime'):
            try:
                date_str = time_elem['datetime'].split('+')[0].split('Z')[0]
                return datetime.fromisoformat(date_str)
            except ValueError:
                pass

        return None

    def _extract_views_count(self, soup: BeautifulSoup) -> int:
        """Извлечь количество просмотров"""
        # Ищем в блоке статистики (dateAndStats)
        statistic_elem = soup.find(class_=re.compile(r'dateAndStats'))
        if statistic_elem:
            # Ищем ячейку с иконкой глаза
            for item in statistic_elem.find_all(class_=re.compile(r'item')):
                # Проверяем есть ли иконка глаза
                svg = item.find('svg')
                if svg:
                    use = svg.find('use')
                    if use and 'eye' in str(use.get('xlink:href', '')):
                        # Нашли ячейку с глазом, извлекаем число
                        text = item.get_text()
                        # Убираем пробелы из числа (4 985 -> 4985)
                        text_clean = re.sub(r'\s+', '', text)
                        match = re.search(r'(\d+)', text_clean)
                        if match:
                            try:
                                return int(match.group(1))
                            except ValueError:
                                pass
        return 0

    def _extract_comments_count(self, soup: BeautifulSoup, article_link: str) -> int:
        """Извлечь количество комментариев из блока статистики новости."""
        article_elem = soup.find("a", href=article_link)
        if not article_elem:
            return 0

        for container in article_elem.parents:
            statistics = container.find(class_=re.compile(r"statistic", re.IGNORECASE), recursive=False)
            if statistics:
                comments_link = (
                    statistics.find("a", href=re.compile(r"/comments/?$"))
                    or statistics.find("a", class_=re.compile(r"comments", re.IGNORECASE))
                )
                count_elem = comments_link.find("span", class_=re.compile(r"text", re.IGNORECASE)) if comments_link else None
                match = re.search(r"\d+", count_elem.get_text()) if count_elem else None
                if match:
                    comments_count = int(match.group())
                    return comments_count
                return 0

        return 0
