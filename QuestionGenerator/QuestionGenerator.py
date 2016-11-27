import json
import random
import sqlalchemy


def uncapitalize(s):
    if len(s) == 0 or s == s.upper():
        return s
    else:
        return s[0].lower() + s[1:]


def find_dict_index_in_list_by_attr(l, key, val):
    res = [c for c in l if c[key] == val]
    if len(res):
        return l.index(res[0])
    else:
        return None


class QuestionGenerator:

    def __init__(self, session_id):
        self.engine = sqlalchemy.create_engine(
            'mysql+pymysql://bestfit:1q2w3e4rt5y6u7@localhost/bestfit', convert_unicode=True)
        self.metadata = sqlalchemy.MetaData(bind=self.engine)
        self.filters = {}
        self.filter_names = {}
        self.questions = {}
        self.products = {}
        self.session = self.load_session(session_id)

    def get_filters(self):
        categories_table = sqlalchemy.Table('categories', self.metadata, autoload=True)
        cat = categories_table.select(categories_table.c.id == self.session['category_id']).execute().first()
        filters_names = cat['filters'].split(',')

        filters_table = sqlalchemy.Table('filters', self.metadata, autoload=True)
        filters_query = filters_table.select(
            sqlalchemy.sql.expression.and_(
                filters_table.c.name.in_(filters_names),
                sqlalchemy.sql.expression.not_(filters_table.c.type == 'KEY')
            )
        )
        filters_rows = filters_query.execute()

        for r in filters_rows:
            f = {}
            for k, v in r.items():
                if k == 'values':
                    f['values'] = json.loads(v)
                    for val in f['values']:
                        val['stats'] = 0
                elif k == 'name':
                    f[k] = v.lower()
                else:
                    f[k] = v

            f['stats'] = 0
            self.filters[f['id']] = f
            self.filter_names[f['name']] = f['id']

    def product_belongs_to_categories(self, product_id):
        products_in_categories_table = sqlalchemy.Table('products_in_categories', self.metadata, autoload=True)
        categories_table = sqlalchemy.Table('categories', self.metadata, autoload=True)
        prods_query = sqlalchemy.sql.expression.join(
            products_in_categories_table, categories_table,
            products_in_categories_table.c.category_id == categories_table.c.id) \
            .select([categories_table.c.id, categories_table.c.name], products_in_categories_table.c.product_id == product_id)

        conn = self.engine.connect()
        prod_rows = conn.execute(prods_query)
        cats = {}
        for v in prod_rows:
            cats[v['id']] = v['name']
        return cats

    def get_questions(self):
        questions_table = sqlalchemy.Table('questions', self.metadata, autoload=True)

        questions_rows = questions_table.select(questions_table.c.filter_id.in_(self.filters.keys())).execute()

        for r in questions_rows:
            q = {}
            for k, v in r.items():
                if k == 'values':
                    q[k] = json.loads(v)
                else:
                    q[k] = v

            self.questions[q['id']] = q

    def get_products(self):
            products_table = sqlalchemy.Table('products', self.metadata, autoload=True)
            products_in_categories_table = sqlalchemy.Table('products_in_categories', self.metadata, autoload=True)

            prods_query = sqlalchemy.sql.expression.join(
                products_in_categories_table, products_table,
                products_in_categories_table.c.product_id == products_table.c.id)\
                .select(products_in_categories_table.c.category_id == self.session['category_id'], use_labels=True)

            conn = self.engine.connect()
            prod_rows = conn.execute(prods_query)

            if 'score' not in self.session['data'].keys():
                self.session['data']['score'] = {}
                self.session['data']['score_additive'] = {}

            for r in prod_rows:
                f = {}
                for k, v in r.items():
                    if k == 'products_data':
                        f[k] = json.loads(v)
                    else:
                        f[k] = v

                if f['products_id'] not in self.session['data']['score'].keys():
                    self.session['data']['score'][f['products_id']] = 1.0
                    self.session['data']['score_additive'][f['products_id']] = 0.0

                for attr in f['products_data']['attributes']:
                    if len(attr['values']):
                        f['products_data'][attr['name'].lower()] = attr['values'][0]

                del f['products_data']['attributes']

                for key in f['products_data']:
                    if key != key.lower():
                        f['products_data'][key.lower()] = f['products_data'][key]
                        del f['products_data'][key]

                price = float(f['products_data']['units'][0]['price']['value'])
                for rng in self.filters[self.filter_names['price']]['values']:
                    lo, hi = rng['key'].split('-')
                    if float(lo) < price < float(hi):
                        f['products_data']['price'] = rng['key']
                        break

                self.products[f['products_id']] = f

    def generate_questions(self):
        random.seed()
        questions_table = sqlalchemy.Table('questions', self.metadata, autoload=True)

        question_text_templates = [
            "Do you prefer the {1} {0}?",
            "Would you like the {0} to be {1}?",
            "Imagine the {0} being {1}. Do you like it?",
            "{0}: {1}?"
            # TODO: add more and look for stupid ones
        ]

        new_questions = []
        for filter_id, filter_data in self.filters.items():
            for value in filter_data['values']:
                new_questions.append({
                    "filter_id": filter_id,
                    'values': json.dumps([value['key']]),
                    'text': random.choice(question_text_templates).format(
                        uncapitalize(filter_data['display_name']).replace('_', ' '),
                        uncapitalize(value['displayName']).replace('_', ' ')
                    ),
                    'custom': 0
                })
        conn = self.engine.connect()
        conn.execute(questions_table.delete()
                     .where(questions_table.c.filter_id in self.filters.keys() and questions_table.c.custom == 0))
        conn.execute(questions_table.insert(), new_questions)

    def load_session(self, session_id):
        sessions_table = sqlalchemy.Table('question_sessions', self.metadata, autoload=True)
        session = {}

        for k, v in sessions_table.select(sessions_table.c.session_id == session_id).execute().first().items():
            if k == 'data':
                if v is None or v == '':
                    v = '{"previous_questions": []}'
                session[k] = json.loads(v)
            else:
                session[k] = v
        return session

    def save_session(self):
        sessions_table = sqlalchemy.Table('question_sessions', self.metadata, autoload=True)
        self.session['data'] = json.dumps(self.session['data'])
        conn = self.engine.connect()
        conn.execute(
            sessions_table.update().where(sessions_table.c.session_id == self.session['session_id']),
            self.session
        )

    def calculate_filters_stats(self, cutoff):
        set_size = 0
        for pk, p in self.products.items():
            if self.session['data']['score'][pk] >= cutoff:
                set_size += 1
            for filter_name in p['products_data'].keys():
                if filter_name in self.filter_names.keys():
                    self.filters[self.filter_names[filter_name]]['stats'] += self.session['data']['score'][pk]
                    e = p['products_data'][filter_name]
                    if not isinstance(e, list):
                        if e in [v['key'] for v in self.filters[self.filter_names[filter_name]]['values']]:
                            result_ind = find_dict_index_in_list_by_attr(self.filters[self.filter_names[filter_name]]['values'], 'key', e)
                            self.filters[self.filter_names[filter_name]]['values'][result_ind]['stats'] += self.session['data']['score'][pk]
        return set_size

    def calculate_questions(self):
        for qk, q in self.questions.items():
            try:
                q['score'] = self.filters[q['filter_id']]['stats'] \
                    / sum([v['stats'] for v in self.filters[q['filter_id']]['values'] if v['key'] in q['values']])
            except ZeroDivisionError:
                q['score'] = 0.0
            if q['score'] < 1e-8 or q['score'] > 1-1e-8:
                self.session['data']['previous_questions'] = [qk] + self.session['data']['previous_questions']


    def get_best_question(self):
        questions = [q for q in self.questions if q['id'] not in self.session['data']['previous_questions']]
        q = min(questions, key=lambda k: abs(k["score"] - 0.5))
        return q

    def calculate_products_scores(self, answer):
        for pk, p in self.products.items():
            q = self.questions[self.session['data']['previous_questions'][-1]]
            prop = self.filters[q['filter_id']]['name']
            if prop in p['products_data'].keys():
                if p['products_data'][prop] not in q['values']:
                    ans = -answer
                else:
                    ans = answer
                self.session['data']['score'][pk] *= (1 + ans) / 2
                self.session['data']['score_additive'][pk] += ans

        max_score = max(self.session['data']['score'].values())
        if max_score:
            for k in self.session['data']['score'].keys():
                self.session['data']['score'][k] /= max_score

    def process_answer(self, answer):
        self.get_filters()
        if answer is None:
             self.generate_questions()
        self.get_questions()
        self.get_products()
        cutoff = 0.1
        set_size = self.calculate_filters_stats(cutoff)
        self.calculate_questions()

        if answer is not None:
            self.calculate_products_scores(answer)
            set_size = self.calculate_filters_stats(cutoff)

        question = self.get_best_question()
        self.session['data']['previous_questions'].append(question['id'])

        other_qs = [q for q in self.questions.values() if q['filter_id'] == question['filter_id']]
        if len(other_qs) == 1:
            self.session['data']['previous_questions'].append(self.questions[other_qs[0]['id']])

        self.save_session()

        if answer is None or (set_size > 1 and len(self.questions) > 1):
            return {"final": False, 'question': question['text']}
        else:
            prod = sorted(self.session['data']['score'], key=lambda k: self.session['data']['score'][k], reverse=True)
            return {
                "final": True,
                "results": prod
            }
