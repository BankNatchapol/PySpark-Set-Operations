[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<p align="center">

  <h3 align="center">PySpark Set Operations</h3>

  <p align="center">
    PySpark all set operation functions.
    <br />
    <br />
    <a href="https://github.com/BankNatchapol/PySpark-Set-Operations/issues">Report Bug</a>
    ·
    <a href="https://github.com/BankNatchapol/PySpark-Set-Operations/issues">Request Feature</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#installation">Installation</a>
    </li>
    <li>
        <a href="#dataset">Dataset</a>
    </li>

<li>
      <a href="#submitting">Submitting</a>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- INSTALLATION -->
## Installation
you can install spark follow this link [Spark](https://spark.apache.org/downloads.html)

<!-- DATASET -->
## Dataset
there are 2 data files. <br>
"in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
> nasa_19950701.tsv <br>

"in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
> nasa_19950801.tsv

<!-- SUBMITTING -->
## Submitting 
there are 6 set operation function files.<br>
### Sample 
get random sample records from the dataset.
> spark-submit nasa_sample.py

### Distinct 
drop the duplicate rows (all columns) from dataset.
> spark-submit nasa_distinct.py

### Union 
combine two dataset’s of the same structure/schema. 
> spark-submit nasa_union.py

### Intersection 
combine two dataset’s of the same structure/schema and remove all duplicate. 
> spark-submit nasa_intersection.py

### Subtract 
get data in first dataset that does not contain in second dataset.  
> spark-submit nasa_subtract.py

### Cartesian 
get Cartesian product of 2 datasets.  
> spark-submit nasa_cartesian.py

<!-- CONTACT -->
## Contact

Facebook - [@Natchapol Patamawisut](https://www.facebook.com/natchapol.patamawisut/)

Project Link: [https://github.com/BankNatchapol/PySpark-Word-Count](https://github.com/BankNatchapol/PySpark-Word-Count)

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/natchapol-patamawisut
