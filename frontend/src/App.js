import React from 'react';
import { useEffect } from 'react';
import "./static/css/style.css"
// import $ from 'jquery'; // Import jQuery if necessary

const SearchEngine = () => {

  useEffect(() => {
    // If there's any specific jQuery initialization in app.js, include it here
    // $.getScript('/static/js/app.js');
  }, []);

  return (
    <div>
      <head>
        <meta charSet="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <link href="https://fonts.googleapis.com/css?family=Poppins:400,500&display=swap" rel="stylesheet" />
        <link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.7.2/css/all.css"
          integrity="sha384-fnmOCqbTlWIlj8LyTjo7mOUStjsKC4pOpQbqyi7RrhN7udi9RwhKkMHpvLbHG9Sr"
          crossOrigin="anonymous"
        />
        <link rel="stylesheet" href="/static/css/style.css" />
        <title>Search Engine</title>
      </head>
      <body>
        <main className="main">
          <img src="/static/images/google.svg" alt="logo here" width="30%" className="logo" />
          <div className="box">
            <input type="image" name="search-boton" src="/static/images/search.jpeg" alt="icon-search" className="icon-search" />
            <input type="text" name="search-text" className="tsearch" id="words" />
          </div>

          <div id="pages" className="container">
            <p>Resultados:</p>
          </div>
        </main>
        <footer>
          <div className="footer">
            <div className="follow">
              <p>Follow us in</p>
              <a href="https://facebook.com">
                <i className="fab fa-facebook"></i>
              </a>
              <a href="https://twitter.com">
                <i className="fab fa-twitter"></i>
              </a>
              <a href="https://linkedin.com">
                <i className="fab fa-linkedin"></i>
              </a>
            </div>
            <p>Copyright &copy; 2020, Designed by Jose</p>
          </div>
        </footer>
        <script src="/static/js/app.js"></script>
      </body>
    </div>
  );
}

export default SearchEngine;



