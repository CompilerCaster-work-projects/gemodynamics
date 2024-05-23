from typing import List, Dict
from datetime import datetime, timedelta
import sys
import os
sys.path.append("./")

import pandas as pd
import numpy as np
import plotly.express as px

from internal.db.db import DB
from query.queries import pipeline


class Gemodynamics:

    def __init__(self, 
                 start_date: datetime, 
                 end_date: datetime,
                 org_ids: List[int],
                 save_path: str):
        """Класс Gemodynamics реализует логику формирования отчета
        'Водители выгрузка типы гемодинамики'.
        
        Основным методом класса является метод run, который выгружает данные из mongo, готовит две страницы отчета
        и сохраняет отчет.

        Args:
            start_date (datetime): Дата начала выгрузки осмотров.
            end_date (datetime): Дата конца выгрузки осмотров.
            org_ids (List[int]): Список id организаций, по которым нужно сделать отчет.   
            save_path (str): Путь до папки, в которую нужно сохранить отчет.

        Example:
            gem_report = Gemodynamics(datetime(2023, 10, 1), datetime(2023, 11, 1))
            gem_report.run()
        """

        # инициализируем экземпляр сервиса работы с базой данных
        self.db = DB(pipeline)

        self.start_date = start_date
        self.end_date = end_date
        self.org_ids = org_ids
        self.save_path = save_path


    def get_reports(self) -> Dict[str, pd.DataFrame]:
        """Метод для получения выгрузок из базы данных.

        Returns:
            Dict[str, pd.DataFrame]: Словарь с датафреймами для обоих страниц отчета.
        """

        # инициализируем словарь
        dfs = dict()

        # запустим выгрузку и сложим в словарь
        df11, df2 = self.db.load(self.start_date, self.end_date, self.org_ids)
        dfs['sheet11'] = df11
        dfs['sheet2'] = df2

        # сохраним csv исходников
        self.db.save('sheet11', df11)
        self.db.save('sheet2', df2)

        # сформируем вторую часть первой страницы
        # это группировка по организации
        df1 = dfs['sheet11'].copy()
        df1 = df1.groupby(['organization_id', 
                                      'organization_inn', 
                                      'organization_name'], as_index=False).agg('sum').drop(['host_release_point'], axis=1)
        dfs['sheet1'] = df1

        # закроем подключение
        self.db.close_client()
        return dfs


    def sheet1_prep(self, dfs: Dict[str, pd.DataFrame]) -> List[pd.DataFrame]:
        """Метод для подготовки первой страницы отчета.

        Args:
            dfs (Dict[str, pd.DataFrame]): Словарь с выгрузками из базы.

        Returns:
            List[pd.DataFrame]: Список с датафремами для заполнения первой страницы отчета.
        """

        # переведем даты в строки
        start_date = self.start_date.strftime("%Y-%m-%d")
        end_date = (self.end_date - timedelta(days=1)).strftime("%Y-%m-%d")

        # подготовим названия колонок
        columns = {
            "count_ad": "АД вне нормы",
            "count_adm_cause": "Недопуски по адм. причинам",
            "count_cancel_cause": "Прерванные осмотры / Зависло",
            "count_med_cause": "Недопуски по мед. причинам",
            "count_medics": "Всего осмотров",
            "count_not_success": "Недопуск (включая тех.сбои)",
            "count_pulse": "ЧСС вне нормы",
            "count_success": "Допуск",
            "count_tech_cause": "Недопуски по тех. причинам",
            "organization_id": "Айди Организации",
            "organization_inn": "ИНН Организации",
            "organization_name": "Организация",
            'host_release_point': "Точка выпуска",
            "Период": "Период"
        }

        res = []
        for sheet_name in ['sheet1', 'sheet11']: # пробежимся по компонентам первой страницы

            buf = dfs[sheet_name].copy()

            # создадим колонки с периодом
            buf['Период'] = start_date + " - " + end_date

            # переименуем колонки
            buf.rename(columns=columns, inplace=True)

            # вычислим относительные величины
            buf["Допуск %"] = ((buf["Допуск"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["Недопуск (включая тех.сбои) %"] = ((buf["Недопуск (включая тех.сбои)"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["Недопуски по мед. причинам %"] = ((buf["Недопуски по мед. причинам"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["Недопуски по адм. причинам %"] = ((buf["Недопуски по адм. причинам"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["Недопуски по тех. причинам %"] = ((buf["Недопуски по тех. причинам"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["Прерванные осмотры / Зависло %"] = ((buf["Прерванные осмотры / Зависло"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["ЧСС вне нормы %"] = ((buf["ЧСС вне нормы"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"
            buf["АД вне нормы %"] = ((buf["АД вне нормы"] / buf["Всего осмотров"] * 100).round(2)).astype(str) + "%"

            # оставим только нужные колонки в первой части
            if sheet_name == 'sheet1':
                buf = buf[["Период",
                            "Организация",
                            "Всего осмотров",
                            "Допуск",
                            "Допуск %",
                            "Недопуск (включая тех.сбои)",
                            "Недопуск (включая тех.сбои) %",
                            "Недопуски по мед. причинам",
                            "Недопуски по мед. причинам %",
                            "Недопуски по адм. причинам",
                            "Недопуски по адм. причинам %",
                            "Недопуски по тех. причинам",
                            "Недопуски по тех. причинам %",
                            "Прерванные осмотры / Зависло",
                            "ЧСС вне нормы",
                            "ЧСС вне нормы %",
                            "АД вне нормы",
                            "АД вне нормы %"]]

            # и во второй
            else:
                buf = buf[["Период",
                            "Организация",
                            "Точка выпуска",
                            "Всего осмотров",
                            "Допуск",
                            "Допуск %",
                            "Недопуск (включая тех.сбои)",
                            "Недопуск (включая тех.сбои) %",
                            "Недопуски по мед. причинам",
                            "Недопуски по мед. причинам %",
                            "Недопуски по адм. причинам",
                            "Недопуски по адм. причинам %",
                            "Недопуски по тех. причинам",
                            "Недопуски по тех. причинам %",
                            "Прерванные осмотры / Зависло",
                            "ЧСС вне нормы",
                            "ЧСС вне нормы %",
                            "АД вне нормы",
                            "АД вне нормы %"]]

            # добавим к результирующему списку
            res.append(buf)
            
        return res


    def sheet2_prep(self, df: pd.DataFrame) -> pd.DataFrame:
        """Метод для подготовки второй страницы отчета.

        Args:
            df (pd.DataFrame): Данные для второй страницы.

        Returns:
            pd.DataFrame: Датафрейм для заполнения второй страницы отчета.  
        """
        
        buf = df.copy()

        # создадим колонки для границы показателей АД и пульса
        buf['boundary_pulse_lower'] = np.nan
        buf['boundary_pulse_upper'] = np.nan
        buf['boundary_sad_lower'] = np.nan
        buf['boundary_sad_upper'] = np.nan
        buf['boundary_dad_upper'] = np.nan
        buf['boundary_dad_lower'] = np.nan

        # вытащим индивидуальные границы по организации
        # buf['boundary'] = buf.boundary.apply(lambda x: x.replace("false", "False").replace('true', 'True') if not pd.isna(x) else x)
        buf.loc[:, 'boundary_pulse_lower'] = buf.apply(lambda x: x.boundary['pulse']['lower'] if x.boundary_origin=='org' else 54, axis=1)
        buf.loc[:, 'boundary_pulse_upper'] = buf.apply(lambda x: x.boundary['pulse']['upper'] if x.boundary_origin=='org' else 100, axis=1)

        buf.loc[:, 'boundary_sad_upper'] = buf.apply(lambda x: x.boundary['pressure']['systolic']['upper'] if x.boundary_origin=='org' else 139, axis=1)
        buf.loc[:, 'boundary_sad_lower'] = buf.apply(lambda x: x.boundary['pressure']['systolic']['lower'] if x.boundary_origin=='org' else 100, axis=1)

        buf.loc[:, 'boundary_dad_upper'] = buf.apply(lambda x: x.boundary['pressure']['diastolic']['upper'] if x.boundary_origin=='org' else 89, axis=1)
        buf.loc[:, 'boundary_dad_lower'] = buf.apply(lambda x: x.boundary['pressure']['systolic']['lower'] if x.boundary_origin=='org' else 60, axis=1)

        # присвоим тип гемодинамики
        buf['type_sad'] = buf.apply(lambda x: "Гипертоник (Повышенное сАД)" if x.mean_sad>x.boundary_sad_upper else ("Гипотоник (Пониженное сАД)" if x.mean_sad<x.boundary_sad_lower else ""), axis=1)
        buf['type_dad'] = buf.apply(lambda x: "Гипертоник (Повышенное дАД)" if x.mean_dad>x.boundary_dad_upper else ("Гипотоник (Пониженное дАД)" if x.mean_dad<x.boundary_dad_lower else ""), axis=1)
        buf['type_pulse'] = buf.apply(lambda x: "Тахиритмик (Повышенная ЧСС)" if x.mean_pulse>x.boundary_pulse_upper else "", axis=1)
        buf['type_sad'] = buf.apply(lambda x: "Брадиритмик (Пониженная ЧСС)" if x.mean_pulse<x.boundary_pulse_lower else "", axis=1)

        # соберем нужные колонки
        buf['Тип гемодинамики'] = buf[['type_sad', 'type_dad', 'type_pulse', 'type_sad']].agg(', '.join, axis=1).str.rstrip(", ").str.lstrip(", ")
        buf['Блок наблюдений по АД'] = buf.apply(lambda x: "Повышенное АД" if x.mean_dad>89 or x.mean_sad>139 else ("Пониженное АД" if x.mean_dad<60 or x.mean_sad<100 else "Нормальное АД"), axis=1)
        buf['Блок наблюдений по ЧСС'] = buf.apply(lambda x: "Повышенная ЧСС" if x.mean_pulse>100 else ("Пониженная ЧСС" if x.mean_pulse<54 else "Нормальная ЧСС"), axis=1)
        buf['Рекомендации'] = buf.apply(lambda x: "Рекомендуется внеочередной медицинский осмотра с заключением врача профпатолога" if x.mean_sad >= 160 or x.mean_sad <= 89 or x.mean_dad >= 100 or x.mean_dad <= 49 or x.mean_pulse >= 106 or x.mean_pulse <= 44 else "Рекомендуется обратиться ко врачу терапевту за медицинской консультацией", axis=1)
        buf['ФИО'] = buf['employee_surname'] + " " + buf['employee_name'] + " " + buf['employee_patronymic']
        buf['Табельный номер'] = buf['employee_number']
        buf['Доля недопусков по АД и ЧСС'] = (buf['count_ad_pulse_cause'] / buf['count_all']) * 100
        buf['Возраст'] = pd.to_datetime(buf['employee_birthday']).apply(lambda x: datetime.now().year - x.year - ((datetime.now().month, datetime.now().day) < (x.month, x.day)))

        # приведем названия в порядок
        buf.rename(columns={
            "count_all": "Всего предрейсовых осмотров",
            "count_ad_pulse_cause": "АД, ЧСС выходят за границы, предрейс",
            "ФИО": "ФИО",
            "Табельный номер": "Табельный номер",
            "mean_sad": "Среднее значение по АД систолическое", 
            "mean_dad": "Среднее значение по АД диастолическое",
            "mean_pulse": "Среднее значение по ЧСС",
            "Тип гемодинамики": "Тип гемодинамики",
            "Рекомендации": "Рекомендации",
            "employee_birthday": "Дата рождения",
            "Доля недопусков по АД и ЧСС": "Доля недопусков по АД и ЧСС"
            }, inplace=True)

        # выделим только нужные колонки
        buf = buf[[
            'organization_id', 'organization_inn', 'organization_name', "ФИО", "Табельный номер",
                "Возраст",
                "Всего предрейсовых осмотров", 
                "АД, ЧСС выходят за границы, предрейс", 
                "Доля недопусков по АД и ЧСС", 
                "Среднее значение по АД систолическое", 
                "Среднее значение по АД диастолическое",
                "Среднее значение по ЧСС",
                "Тип гемодинамики",
                "Рекомендации",
                'Блок наблюдений по АД',
                'Блок наблюдений по ЧСС']]

        # округлим некоторые поля
        buf['Среднее значение по АД систолическое'] = buf['Среднее значение по АД систолическое'].round(2)
        buf['Среднее значение по АД диастолическое'] = buf['Среднее значение по АД диастолическое'].round(2)
        buf['Среднее значение по ЧСС'] = buf['Среднее значение по ЧСС'].round(2)
        buf["Доля недопусков по АД и ЧСС"] = buf["Доля недопусков по АД и ЧСС"].round(2).astype(str) + "%"

        return buf


    def draw(self, df: pd.DataFrame):
        """Метод для генерации графиков типа 'Ящик с усами' для показателей АД и пульса.

        Args:
            df (pd.DataFrame): Данные для генерации графиков.
        """

        os.makedirs("./images", exist_ok=True)
        for i in df.organization_name.unique():
            # график для систолического АД
            fig1 = px.box(df[df['organization_name']==i], 
                        x="Блок наблюдений по АД", 
                        y="Среднее значение по АД систолическое",
                        color="Блок наблюдений по АД",
                        color_discrete_map={
                            "Нормальное АД": "green",
                            "Повышенное АД": "red",
                            "Пониженное АД": "yellow"
                        },
                        points='all',
                        width=1000, 
                        height=700,
                        title='Систолическое давление')
            fig1.write_image(f"images/{i}_fig1.png")

            # график для диастолического АД
            fig2 = px.box(df[df['organization_name']==i], 
                        x="Блок наблюдений по АД", 
                        y="Среднее значение по АД диастолическое",
                        color="Блок наблюдений по АД",
                        color_discrete_map={
                            "Нормальное АД": "green",
                            "Повышенное АД": "red",
                            "Пониженное АД": "yellow"
                        },
                        points='all',
                        width=1000, 
                        height=700,
                        title='Диастолическое давление')
            fig2.write_image(f"images/{i}_fig2.png")

            # график для ЧСС
            fig3 = px.box(df[df['organization_name']==i], 
                        x="Блок наблюдений по ЧСС", 
                        y="Среднее значение по ЧСС",
                        color="Блок наблюдений по ЧСС",
                        color_discrete_map={
                            "Нормальная ЧСС": "green",
                            "Повышенная ЧСС": "red",
                            "Пониженная ЧСС": "yellow"
                        },
                        points='all',
                        width=1000, 
                        height=700,
                        title='Частота сердечно-сосудистых сокращений')
            fig3.write_image(f"images/{i}_fig3.png")


    def save(self, df1: pd.DataFrame, df11: pd.DataFrame, df2: pd.DataFrame):
        """Метод для формирования отчета и сохранения.

        Args:
            df1 (pd.DataFrame): Данные для первой части первой страницы.
            df11 (pd.DataFrame): Данные для второй части первой страницы.
            df2 (pd.DataFrame): Данные для второй страницы.
        """

        for i in df1.Организация.unique(): # пробежимся по всем организациям

            # выделим названия и создадим генератор отчета
            idd = df2.loc[df2.organization_name==i, 'organization_name'].values[0]
            os.makedirs(self.save_path, exist_ok=True)
            writer = pd.ExcelWriter(f"{self.save_path}/Водители выгрузка типы гемодинамики {i}.xlsx", engine='xlsxwriter')
            workbook = writer.book

            # выделим отдельные организации
            buf1 = df1[df1.Организация==i].reset_index(drop=True)
            buf11 = df11[df11.Организация==i].drop(['Организация'], axis=1).reset_index(drop=True)
            buf2 = df2[(df2.organization_name==i) & (df2['Тип гемодинамики']!='')].drop(['organization_name', 'organization_id', 'organization_inn', 'Блок наблюдений по АД', 'Блок наблюдений по ЧСС'], axis=1)

            # укажем расположение данных на странице
            buf1.to_excel(writer, sheet_name="Лист1", index=False)  # send df to writer
            buf11.to_excel(writer, sheet_name="Лист1", index=False, startrow=len(buf1)+1, startcol=0)

            # раскрасим колонку с недопусками
            worksheet = writer.sheets["Лист1"]  # pull worksheet object    
            for i, v in enumerate(buf1['Недопуск (включая тех.сбои) %'].values):
                if float(v.replace("%",'')) < 25:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('green')
                elif float(v.replace("%",'')) < 35:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('yellow')
                else:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('red')
                worksheet.write(i+1, 6, v, cell_format)
                
            for i, v in enumerate(buf11['Недопуск (включая тех.сбои) %'].values):
                if float(v.replace("%",'')) < 25:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('green')
                elif float(v.replace("%",'')) < 35:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('yellow')
                else:
                    cell_format = workbook.add_format()
                    cell_format.set_bg_color('red')
                worksheet.write(i+1+len(buf1)+1, 6, v, cell_format)

            # добавим текст с рекомендациями по отчету
            worksheet.write_string(len(buf1) + len(buf11) + 3, 0, "Рекомендуемый период составляет 1 месяц")
            worksheet.write_string(len(buf1) + len(buf11) + 4, 0, "если меньше - данные теряют точность, если больше данные теряют актуальность")

            # отнормируем ширину колонок
            for idx, col in enumerate(buf1):
                series = buf1[col]
                max_len = max((
                    series.astype(str).apply(len).max(),
                    len(str(col))
                    ))+1
                worksheet.set_column(idx, idx, max_len)
            for idx, col in enumerate(buf11):
                series = buf11[col]
                max_len = max((
                    series.astype(str).apply(len).max(),
                    len(str(col))
                    ))+1
                worksheet.set_column(idx, idx, max_len)

            # укажем расположение данных на странице 2
            buf2.to_excel(writer, sheet_name='Лист2', index=False)
            worksheet = writer.sheets["Лист2"]
            for idx, col in enumerate(buf2):
                series = buf2[col]
                max_len = max((
                    series.astype(str).apply(len).max(),
                    len(str(col))
                    ))+1
                worksheet.set_column(idx, idx, max_len)

            # вставим графики
            worksheet.insert_image(1, 14, f"images/{idd}_fig1.png")   
            worksheet.insert_image(38, 14, f"images/{idd}_fig2.png")   
            worksheet.insert_image(74, 14, f"images/{idd}_fig3.png")   

            # сохраним отчет
            writer.close()


    def run(self):
        """Главный метод класса.
        Запускает все процессы по очереди.

        Raises:
            exc: Исключение.
        """
        
        try:
            # получим данные из базы
            dfs = self.get_reports()

            # подготовим данные для первой страницы
            df1, df11 = self.sheet1_prep(dfs)

            # подготовим данные для второй
            df2 = self.sheet2_prep(dfs['sheet2'])

            # сохраним отчет
            self.save(df1, df11, df2)
        except Exception as exc:
            raise exc


if __name__ == "__main__":
    start_date = datetime(2023, 10, 1)
    end_date = datetime(2023, 11, 1) # ! дата окончания не входит в интервал [start_date, end_date)

    org_ids = [1328, 2211, 452, 836, 844, 747, 857, 810, 166]
    
    gem_report = Gemodynamics(
        start_date=start_date,
        end_date=end_date,
        org_ids=org_ids,
        save_path="results"
    )

    gem_report.run()