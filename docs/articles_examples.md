# Example Articles for Validation

These are real articles found in the last 2 weeks (as of Feb 2026) that the system should detect.
Use these to validate that scrapers and classifiers are working correctly.

## MVP Sources (must find these)

### Mako (scraper)
1. https://www.mako.co.il/men-men_news/Article-8d6023273874c91027.htm
2. https://www.mako.co.il/men-men_news/Article-ee9aac95eb64c91026.htm
3. https://www.mako.co.il/men-men_news/Article-326411394f9fb91026.htm
4. https://www.mako.co.il/men-men_news/Article-921a9373b4aeb91026.htm

### Maariv (scraper)
8. https://www.maariv.co.il/news/law/article-1270778

### Ynet (RSS)
9. https://www.ynet.co.il/news/article/rkpeb8zbwg

## Other Sources (future expansion)

### Israel Hayom
4. https://www.israelhayom.co.il/news/crime/article/19757865
5. https://www.israelhayom.co.il/news/crime/article/19850947

### Local News Sites
3. https://news-desk.co.il/news/פרשת-ניצול-מחרידה-בירושלים-נהג-אוטובו/
4. https://newzim.co.il/merkaz/?p=70142
6. https://www.kan-ashkelon.co.il/news/100735
7. https://www.hashikma-batyam.co.il/criminal_law/42164/
7. https://nessziona.net/חדשות/החשד-סחר-בנשים-בלב-בת-ים-צפו-בתיעוד-המעצר-והפשיטה-735781

## Cross-Source Stories

Some stories appear on multiple sites (deduplication test):
- Story #4: newzim + mako + israelhayom (same incident)
- Story #7: hashikma-batyam + mako + nessziona (same incident)

## Notes

- Mako "men-men_news" section contains crime/law enforcement articles
- Maariv "news/law" section has legal/crime coverage
- Many articles are cross-published across sites within hours
- Local news sites often break stories first, then picked up by majors
