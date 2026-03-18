# Conversation

## User
Is it possible to scrape online news websites (such as ynet, Haaretz, Walla, etc.) on a topic of public interest (for example, human trafficking) and publish a database of articles on the topic?

## Assistant
Yes, but **not freely and not without legal risk**.

In Israel, the mere fact that a subject is “of public interest” **does not create a general exemption** allowing you to scrape content from news sites and republish it. Copyright law protects the work itself, while **facts, data, and “news of the day” as such are not protected**, and the originality of a “collection” or database is generally assessed mainly according to its **selection and arrangement**. At the same time, fair use exists in Israel, but it is context-dependent and is examined, among other things, according to the purpose of the use, its scope, and its market effect. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/3953.htm))

Accordingly, the key distinction is between two models:

**The riskier model:**  
Scraping and republishing full article text, long paragraphs, images, many headlines on a significant scale, or a database that effectively serves as a “substitute” for the news sites. This may be considered copyright infringement, and it will certainly be exposed to contractual claims under the sites’ terms of use. ynet, for example, expressly prohibits the use of crawlers/robots for automatic searching, scanning, copying, or retrieval, and also prohibits creating a compilation/collection/database containing content originating from the site. Haaretz likewise prohibits scraping, mining, collecting, and copying information from the site in its terms of use. ([ynet.co.il](https://www.ynet.co.il/article/bkswwa3sn))

**The relatively safer model:**  
Building a **metadata-only** database: URL, site name, date, topic/tags, entities, an original summary written by you, and perhaps very short quotations where there is a clear justification. This rests on the principle that facts and data are not protected as such, but one must still be careful not to “extract” protected material or violate terms of use. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/3953.htm))

There is also a **privacy** layer. If the database includes names of victims, minors, witnesses, suspects, or sensitive information that is aggregated in a way that amplifies exposure, questions may arise under privacy law, and personal data repositories may also trigger additional regulatory obligations. ([gov.il](https://www.gov.il/he/pages/privacy_law?utm_source=chatgpt.com))

The practical conclusion:
- **Yes**, in principle it is possible to build and publish a database on a topic of public interest.
- **It is not recommended** to base it on scraping and republishing full or extensive article content.
- **It is recommended** to base it on metadata, links to the source, classifications, and entities, using your own summaries rather than the site’s text.
- It is strongly preferable to review each site individually: terms of use, robots.txt, and whether there is a suitable API/RSS feed/license.

If the goal is a public-interest / research / nonprofit project, the conservative path is:
1. Collect URLs and factual data only  
2. Keep internal snapshots for research purposes only, not for publication  
3. Publicly publish metadata + links + tags + an independent summary  
4. Maintain a takedown/correction procedure  
5. Obtain a brief legal review before launch

If you want, I can draft a **practical and relatively safe operating policy** for such a project in Israel, or a **legal/technical checklist** before collection and publication.

## User
What about documents produced by official bodies such as the police, the courts, and others? What types of such documents exist and are publicly available on issues of prostitution and human trafficking, and what is the legal position regarding collecting them into a dataset?

## Assistant
Yes — but one must distinguish sharply between **different types of documents**. Legally, not every “official document” is the same.

The most important rule is this: under section 6 of the Israeli Copyright Act, **there is no copyright** in **laws, regulations, Knesset proceedings, and judicial decisions** of a court or of a governmental authority exercising adjudicative power. By contrast, other state documents — for example reports, policy papers, booklets, press releases, procedures, and studies — may absolutely be protected, and the gov.il terms of use also generally prohibit copying and republishing protected material without consent, subject to applicable law. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/3953.htm))

Accordingly, for dataset purposes, I would divide things as follows:

## 1) Documents that can generally be collected and republished in full text
These are the legally “easiest” documents:

**Legislation and legislative materials**
- Laws and regulations
- Bills and explanatory notes
- Knesset proceedings and committee protocols

**Judicial decisions**
- Judgments and decisions of the courts
- Decisions of quasi-judicial bodies, insofar as they fall within the definition in section 6

These categories fall under the section 6 exception, and therefore, from a copyright perspective, it is generally possible to build a textual corpus from them. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/3953.htm))

Even here, however, there are other constraints:
- Privacy and publication restrictions
- Anonymization already performed by the adjudicating body
- Special restrictions relating to minors, victims of sexual offences, and closed-door proceedings

For example, youth court proceedings are held in camera by default. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/70348.htm?utm_source=chatgpt.com))

## 2) Official documents that can be collected for research, but not necessarily republished in full text
This is where most of the “governmental” material you are probably looking for is found:

- Annual reports of the governmental unit coordinating the fight against human trafficking  
- Government studies on the implementation of the law prohibiting the consumption of prostitution  
- State Attorney’s Office guidelines / enforcement guidelines  
- Training booklets, position papers, inter-ministerial reviews  
- Knesset Research and Information Center documents  
- Police / prosecution / ministry press releases

There are currently, for example:
- official annual reports of the coordinating unit against human trafficking for 2022–2023; ([gov.il](https://www.gov.il/he/pages/trafficking-report-2022?utm_source=chatgpt.com))
- State Attorney guideline 2.2 on enforcement policy regarding offences associated with involvement in prostitution; ([gov.il](https://www.gov.il/BlobFolder/dynamiccollectorresultitem/002-2/he/s-a-guidelines_2.2.pdf?utm_source=chatgpt.com))
- the accompanying research on the implementation of the law prohibiting the consumption of prostitution, published in 2025; ([fs.knesset.gov.il](https://fs.knesset.gov.il/25/Committees/25_cs_bg_5993752.pdf?utm_source=chatgpt.com))
- Knesset Research and Information Center documents on prostitution, human trafficking, and enforcement of the prohibition on the consumption of prostitution. ([fs.knesset.gov.il](https://fs.knesset.gov.il/globaldocs/MMM/0b4d6b58-e9f7-e411-80c8-00155d010977/2_0b4d6b58-e9f7-e411-80c8-00155d010977_11_6807.pdf?utm_source=chatgpt.com))

These documents are **publicly available**, but that does not necessarily mean it is permissible to take their PDF/HTML files, duplicate them as-is, and republish them as part of an open dataset. Since these are generally state works, initial copyright ownership usually belongs to the state, and the default term of protection for a state work is 50 years from creation. ([nevo.co.il](https://www.nevo.co.il/law_html/law00/3953.htm))

Accordingly, for documents of this type, the conservative route is:
- Collect the documents
- Extract **metadata**, data points, cataloging information, entities, and tags from them
- Publish **links, dates, publishing body, document type, subject matter, and identifiers**
- And where text is needed: publish only **very short excerpts** or summaries written by you

## 3) Documents with particularly high risk
Here much greater caution is required:

- Indictments
- Remand / detention applications
- Affidavits, expert opinions, appendices
- Investigative materials
- Non-public protocols
- Documents obtained through freedom of information procedures
- Documents containing identifying details of victims, witnesses, minors, migrant workers, or victims of sexual offences

These usually **do not** fall within the section 6 exception, because they are not “judicial decisions” but rather documents of litigants or authorities. Beyond that, even if you obtained access to them, they may still be subject to privacy restrictions, privilege/confidentiality, or terms of use. As to personal data, the Israeli Privacy Protection Authority emphasizes that database owners have duties and restrictions, and in some cases obligations of notice/registration and database documentation updates may also apply. ([gov.il](https://www.gov.il/he/departments/general/manager_duties?utm_source=chatgpt.com))

## What types of public documents actually exist on prostitution and human trafficking
In practice, in Israel it is possible to build a fairly rich database from the following materials:

**Legislation and legislative proceedings**
- The law prohibiting the consumption of prostitution and its extensions/amendments
- Penal Code provisions on human trafficking, pimping, causing prostitution, and holding a person under conditions of slavery
- Bills, explanatory notes, and committee discussions

**Case law**
- Judgments and decisions in human trafficking cases
- Administrative / constitutional decisions on recognition of trafficking victims or state policy  
For example, there are publicly available Supreme Court decisions on human trafficking and recognition of trafficking victims. ([gov.il](https://www.gov.il/BlobFolder/dynamiccollectorresultitem/decision1591-18/he/1591-18.pdf?utm_source=chatgpt.com))

**Government reports**
- Reports of the unit coordinating the fight against human trafficking
- Historical governmental reviews on the subject
- Professional booklets for identifying and treating trafficking victims ([gov.il](https://www.gov.il/BlobFolder/dynamiccollectorresultitem/db1212/he/trafficking_report_2023.pdf?utm_source=chatgpt.com))

**Guidelines and procedures**
- State Attorney guidelines on prostitution and trafficking
- Police orders/procedures published to the public, or at least parts of them
- Victims’ rights documents of the police and the judiciary ([gov.il](https://www.gov.il/BlobFolder/dynamiccollectorresultitem/002-2/he/s-a-guidelines_2.2.pdf?utm_source=chatgpt.com))

**Public studies and reviews**
- Knesset Research and Information Center documents
- Implementation studies of the law prohibiting the consumption of prostitution
- Comparative reviews and preparatory documents for Knesset committees ([fs.knesset.gov.il](https://fs.knesset.gov.il/25/Committees/25_cs_bg_5993752.pdf?utm_source=chatgpt.com))

## So what can actually be built in practice?
The safest model is:

**Relatively permissible and relatively safe**
- A dataset of legislation, Knesset protocols, judgments, and judicial decisions
- A metadata dataset about other governmental documents
- A layer of tagging/classification/NER/topics/timelines created by you

**Possible, but requiring a license / further review**
- An open dataset containing full text of governmental reports, booklets, procedures, studies, and press releases

**Not recommended without specific review**
- Indictments, investigative materials, victim documentation, cases involving minors, sexual offence cases, or documents containing identifying or otherwise sensitive personal details

## Practical recommendation
If the goal is a public-interest / research project on prostitution and human trafficking, I would structure it in 3 layers:

1. **Fully open layer**  
   Legislation, Knesset proceedings, protocols, judgments, and judicial decisions only.

2. **Open metadata layer**  
   For all other documents: title, date, publishing body, URL, document type, independent summary, and tags.

3. **Internal research layer**  
   Full copies of governmental PDF/HTML documents for analytical purposes only, without public republication, until a license or legal opinion is obtained.

That is probably the most reasonable legal and practical structure.
