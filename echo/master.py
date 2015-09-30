from tectonic.proctor import Proctor

Proctor((['python', '-m', 'tectonic.bureaucrat'],
         ['python', '-m', 'echo'],
         ['python', '-m', 'echo'],
         ['python', '-m', 'thing1'],
         ['python', '-m', 'thing2']), log_dir='logs').run()
