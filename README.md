# Lord Stanley's Cup - Fantasy Hockey Webapp

A fantasy hockey standings web app. Four owners each draft 8 NHL teams. One cup moves between teams based on game results — the winner of each cup game keeps the cup until their next game.
Built with Python, Flask, and the NHL.com API.

## How It Works

Each season, the previous year's Stanley Cup winner starts with the cup. A game is a "cup game" if one of the teams currently holds the cup. The winner of a cup game earns a point and holds the cup until their next game.

## Setup
### Requirements

Python 3.13+  
[Poetry](https://python-poetry.org/docs/#installation)  

#### Install from bash terminal:

git clone https://github.com/E-Evenson/lordstanley.git  
cd lordstanley  
poetry install  

#### Run:

poetry run flask --app src/lord_stanley/app.py run  

Then open http://127.0.0.1:5000 in your browser.

## To-Do

Logging  
Error Handling  
Documentation  
Dockerize  
Tests