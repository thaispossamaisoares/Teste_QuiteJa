from flask import Flask, request, jsonify
import csv

app = Flask(__name__)

# Carregar os tipos do arquivo tipos.csv em um dicionário
tipos_dict = {}
with open('tipos.csv', newline='') as arquivo_tipos:
    leitor_tipos = csv.reader(arquivo_tipos)
    next(leitor_tipos)  # Ignorar cabeçalho
    for linha in leitor_tipos:
        tipo_id = int(linha[0])
        nome_tipo = linha[1]
        tipos_dict[tipo_id] = nome_tipo

# Definir a rota da API para retornar o tipo com base no ID
@app.route('/tipo/<int:id>', methods=['GET'])
def obter_tipo(id):
    if id in tipos_dict:
        return jsonify({'tipo': tipos_dict[id]})
    else:
        return jsonify({'mensagem': 'ID de tipo não encontrado'}), 404

if __name__ == '__main__':
    app.run(debug=True)

# usar para teste: http://localhost:5000/tipo/1
