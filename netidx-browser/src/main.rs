use iced::{
    widget::{button, column, text, Column},
    Alignment, Element,
};

#[derive(Debug, Clone, Copy, Default)]
struct Counter {
    value: i64,
}

#[derive(Debug, Clone, Copy)]
enum Message {
    Inc,
    Dec,
    Create,
    Delete,
}

impl Counter {
    fn update(&mut self, m: Message) {
        match m {
            Message::Inc => self.value += 1,
            Message::Dec => self.value -= 1,
            Message::Create | Message::Delete => (),
        }
    }

    fn view(&self) -> Column<Message> {
        column![
            button("inc").on_press(Message::Inc),
            text(self.value).size(50),
            button("dec").on_press(Message::Dec)
        ]
        .padding(20)
        .align_x(Alignment::Center)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct MaybeCounter {
    value: Option<Counter>,
}

impl MaybeCounter {
    fn update(&mut self, m: Message) {
        match m {
            Message::Create => self.value = Some(Default::default()),
            Message::Delete => self.value = None,
            Message::Inc | Message::Dec => {
                if let Some(c) = &mut self.value {
                    c.update(m)
                }
            }
        }
    }

    fn view(&self) -> Element<Message> {
        match self.value.as_ref() {
            None => column![
                button("create").on_press(Message::Create),
                button("destroy").on_press(Message::Delete)
            ]
            .into(),
            Some(c) => column![
                button("create").on_press(Message::Create),
                c.view(),
                button("destroy").on_press(Message::Delete)
            ]
            .into(),
        }
    }
}

fn main() -> iced::Result {
    iced::run("A Counter", MaybeCounter::update, MaybeCounter::view)
}
