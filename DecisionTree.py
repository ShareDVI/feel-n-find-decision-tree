from flask import Flask
from QuestionGenerator import QuestionGenerator
from flask import request
import json
app = Flask(__name__)


@app.route('/')
def api_root():
    return """First request GET /question(session_id, category_id),
    then POST  /question (session_id) with answer as post data"""


@app.route('/question/<session_id>', methods=['GET'])
def api_question_init(session_id):
    gen = QuestionGenerator.QuestionGenerator(session_id)
    return json.dumps(gen.process_answer(answer=None))


@app.route('/question/<session_id>', methods=['POST'])
def api_question(session_id):
    gen = QuestionGenerator.QuestionGenerator(session_id)
    answer = request.form['answer']
    return json.dumps(gen.process_answer(answer))

if __name__ == '__main__':
    app.run()
